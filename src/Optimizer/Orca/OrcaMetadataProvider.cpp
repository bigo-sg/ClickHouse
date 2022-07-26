#include <memory>
#include <mutex>
#include <ctime>
#include <Optimizer/Orca/OrcaMetadataProvider.h>
#include <gpos/common/CAutoP.h>
#include <gpos/common/CAutoRef.h>
#include <gpos/error/CAutoTrace.h>
#include <gpos/io/COstreamString.h>
#include <gpos/memory/CMemoryPool.h>
#include <gpos/task/CWorker.h>

#include <gpopt/mdcache/CMDAccessor.h>
#include <naucrates/dxl/CDXLUtils.h>
#include <naucrates/exception.h>
#include <naucrates/md/CMDColumn.h>
#include <naucrates/md/CDXLColStats.h>
#include <naucrates/md/CDXLRelStats.h>
#include <naucrates/md/CMDTypeBoolGPDB.h>
#include <naucrates/md/CMDTypeInt4GPDB.h>
#include <naucrates/md/CMDTypeInt8GPDB.h>
#include <naucrates/md/CMDRelationExternalGPDB.h>
#include <Optimizer/Orca/OrcaUtil.h>
#include <Optimizer/Stats/StatisticsAccessor.h>

#include <Common/logger_useful.h>
#include "Optimizer/Stats/StatisticsData.h"
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Optimizer/Orca/CHToOrcaUtil.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_NOT_FOUND;
}

static std::once_flag init_once_flag;
OrcaMetadataProvider::OrcaMetadataProvider(CMemoryPool * mem_pool, MetadataObjectRetrieverPtr static_metadata_retriver, ContextPtr context, ASTPtr query)
{
    std::call_once(init_once_flag, []() { OrcaMetadataProvider::initRelObjBuilders(); });
    object_retriever = std::make_shared<MetadataObjectRetriever>();
    *object_retriever = *static_metadata_retriver;
    loadDynamicMetadataObjects(mem_pool, context, query);
}

OrcaMetadataProvider::~OrcaMetadataProvider() = default;

gpos::CWStringBase * OrcaMetadataProvider::GetMDObjDXLStr(
    CMemoryPool * mem_pool,
    CMDAccessor *, //md_accessor
    IMDId * mdid) const
{
    auto obj = object_retriever->getObjectById(IMDIdRef{mdid});

    // result string
    gpos::CAutoP<CWStringDynamic> result;

    result = nullptr;

    if (obj.get() == nullptr)
    {
        // Relstats and colstats are special as they may not
        // exist in the metadata file. Provider must return dummy objects
        // in this case.
        switch (mdid->MdidType())
        {
            case IMDId::EmdidRelStats:
            {
                mdid->AddRef();
                CAutoRef<gpmd::CDXLRelStats> relation_stats;
                relation_stats = gpdxl::CDXLRelStats::CreateDXLDummyRelStats(mem_pool, mdid);
                result = gpdxl::CDXLUtils::SerializeMDObj(mem_pool, relation_stats.Value(), true /*fSerializeHeaders*/, false /*findent*/);
                break;
            }
            case IMDId::EmdidColStats:
            {
                CAutoP<CWStringDynamic> name_str;
                name_str = GPOS_NEW(mem_pool) CWStringDynamic(mem_pool, mdid->GetBuffer());
                CAutoP<gpdxl::CMDName> name;
                name = GPOS_NEW(mem_pool) gpdxl::CMDName(mem_pool, name_str.Value());
                mdid->AddRef();
                CAutoRef<gpdxl::CDXLColStats> col_stats;
                col_stats = gpdxl::CDXLColStats::CreateDXLDummyColStats(
                    mem_pool, mdid, name.Value(), gpdxl::CStatistics::DefaultColumnWidth /* width */);
                name.Reset();
                result = gpdxl::CDXLUtils::SerializeMDObj(mem_pool, col_stats.Value(), true /*fSerializeHeaders*/, false /*findent*/);
                break;
            }
            default:
            {
                GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
            }
        }
    }
    else
    {
        // copy string into result
        result = gpdxl::CDXLUtils::SerializeMDObj(mem_pool, obj.get(), true, false);
    }

    GPOS_ASSERT(nullptr != result.Value())

    return result.Reset();
}

gpmd::IMDId * OrcaMetadataProvider::MDId(CMemoryPool * mp, CSystemId sysid, IMDType::ETypeInfo type_info) const
{
    return GetGPDBTypeMdid(mp, sysid, type_info);
}

void OrcaMetadataProvider::loadDynamicMetadataObjects(CMemoryPool * mem_pool, ContextPtr context, ASTPtr query)
{
    auto & stats_accessor = StatisticsAccessor::instance();
    auto rels_stats = stats_accessor.getStatistics(context, query);
    for (auto & rel_stats : rels_stats)
    {
        auto rel_obj_builder = getRelObjBuilder(rel_stats->getStorage());
        if (!rel_obj_builder)
        {
            String msg = String("Unsupport storage ") + rel_stats->getStorage()->getName();
            GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiDXLValidationError, msg.c_str());
        }

        auto rel_obj = (*rel_obj_builder)(mem_pool, this, rel_stats);
        object_retriever->addMetadataCacheObject(rel_obj);
        LOG_DEBUG(logger, "build rel obj:{}", serializeMDObj(rel_obj.get()));
        auto rel_stats_obj = buildRelStatsObj(mem_pool, dynamic_cast<gpmd::CMDIdGPDB*>(rel_obj->MDId()), rel_stats);
        object_retriever->addMetadataCacheObject(rel_stats_obj);
        LOG_DEBUG(logger, "build rel stats obj:{}", serializeMDObj(rel_stats_obj.get()));

        for (Int32 i = 1, sz = rel_stats->getPhysicalColumns().size(); i <= sz; ++i)
        {
            const auto & col_stats = rel_stats->getPhysicalColumns()[i - 1];
            auto col_stats_obj = buildColStatsObj(mem_pool, dynamic_cast<gpmd::CMDIdGPDB*>(rel_obj->MDId()), col_stats, i);
            object_retriever->addMetadataCacheObject(col_stats_obj);
            LOG_DEBUG(logger, "build col stats obj:{}", serializeMDObj(col_stats_obj.get()));
        }
        for (Int32 i = 0, sz0 = rel_stats->getPhysicalColumns().size(), sz = rel_stats->getHiddenColumns().size(); i < sz; ++i)
        {
            object_retriever->addMetadataCacheObject(buildColStatsObj(mem_pool, dynamic_cast<gpmd::CMDIdGPDB*>(rel_obj->MDId()), rel_stats->getHiddenColumns()[i], sz0 + i));
        }
    }
}

std::map<String, OrcaMetadataProvider::RelObjectBuilder> OrcaMetadataProvider::rel_obj_builders;
void OrcaMetadataProvider::initRelObjBuilders()
{
    //rel_obj_builders["HiveCluster"]
    rel_obj_builders["MergeTree"]
        = [](CMemoryPool * mp, OrcaMetadataProvider * provider, TableStatisticsPtr rel_stats) -> IMDCacheObjectRef
    { 
        auto storage = rel_stats->getStorage();
        String table_id = storage->getStorageID().table_name;
        if (!storage->getStorageID().database_name.empty())
        {
            table_id = storage->getStorageID().database_name + "." + table_id;
        }

        auto rel_id_ref = makeMDIdGPDB(mp, provider->id_allocator.nextRelationId());
        auto * rel_id = rel_id_ref.get();
        rel_id->AddRef();

        gpmd::CMDColumnArray * col_array = GPOS_NEW(mp) gpmd::CMDColumnArray(mp);
        auto build_col_array = [&](const ColumnStatisticsList & cols_stats, bool is_hidden, gpmd::CMDColumnArray * col_array_)
        {
            for (Int32 i = 1, sz = cols_stats.size(); i <= sz; ++i)
            {
                const auto & col_stats = cols_stats[i - 1];
                auto md_type_id_obj = provider->object_retriever->getTypeObject(CHToOrcaUtil::extracOrcaTypeName(col_stats->type));
                if (md_type_id_obj.get() == nullptr)
                {
                    String msg = String("Unsupport type ") + col_stats->type->getName();
                    GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiDXLValidationError, msg.c_str());
                }
                auto * md_type_id = md_type_id_obj->MDId();
                Int32 attrno = is_hidden ? -i : i;
                auto * col_obj
                    = buildColObj(mp, col_stats->name_parts, attrno, md_type_id, true, CHToOrcaUtil::extracOrcaTypeWidth(col_stats->type));
                col_array_->Append(col_obj);
            }
        };
        build_col_array(rel_stats->getPhysicalColumns(), false, col_array);
        build_col_array(rel_stats->getHiddenColumns(), true, col_array);
        col_array->AddRef();

        gpdxl::CMDIndexInfoArray * md_index_info_array = GPOS_NEW(mp) gpdxl::CMDIndexInfoArray(mp);
        md_index_info_array->AddRef();
        gpdxl::IMdIdArray * mdid_trigger_array = GPOS_NEW(mp) gpdxl::IMdIdArray(mp);
        mdid_trigger_array->AddRef();
        gpdxl::IMdIdArray * mdid_check_constraint_array = GPOS_NEW(mp) gpdxl::IMdIdArray(mp);
        mdid_check_constraint_array->AddRef();
        gpdxl::CMDRelationExternalGPDB * rel_obj = GPOS_NEW(mp) gpdxl::CMDRelationExternalGPDB(
            mp,
            rel_id,
            makeMDName(mp, table_id),
            gpdxl::IMDRelation::EreldistrRandom,
            col_array,
            nullptr, /*distr_col_array*/
            false, /*convert_hash_to_random*/
            nullptr, /*keyset_array*/
            md_index_info_array, /*md_index_info_array*/
            mdid_trigger_array, /*mdid_triggers_array*/
            mdid_check_constraint_array, /*mdid_check_constraint_array*/
            -1, /*reject_limit*/
            false, /*is_reject_limit_in_rows*/
            nullptr /*mdid_fmt_err_table*/); 
        return IMDCacheObjectRef(rel_obj); 
    };

    // other storage types

}

std::optional<OrcaMetadataProvider::RelObjectBuilder> OrcaMetadataProvider::getRelObjBuilder(StoragePtr storage)
{
    auto it = rel_obj_builders.find(storage->getName());
    if (it == rel_obj_builders.end())
        return {};
    return it->second;
}

gpmd::CMDColumn *
OrcaMetadataProvider::buildColObj(CMemoryPool * mp, const Strings & names, Int32 attrnum, gpmd::IMDId * md_type_id, bool is_nullable, Int32 length)
{
    String name;
    for (const auto & n : names)
    {
        if (!name.empty())
            name += ".";
        name += n;
    }
    md_type_id->AddRef();
    gpmd::CMDColumn * col = GPOS_NEW(mp) gpmd::CMDColumn(makeMDName(mp, name), attrnum, md_type_id, -1, is_nullable, false, nullptr, length);
    return col;
}

IMDCacheObjectRef OrcaMetadataProvider::buildRelStatsObj(CMemoryPool * mp, gpmd::CMDIdGPDB * rel_mdid, TableStatisticsPtr rel_stats)
{
    auto storage = rel_stats->getStorage();
    String table_id = storage->getStorageID().table_name;
    if (!storage->getStorageID().database_name.empty())
    {
        table_id = storage->getStorageID().database_name + "." + table_id;
    }

    rel_mdid->AddRef();
    gpmd::CMDIdRelStats * rel_stats_id = GPOS_NEW(mp) gpmd::CMDIdRelStats(rel_mdid);
    rel_stats_id->AddRef();
    gpmd::CDXLRelStats * rel_stats_obj = GPOS_NEW(mp) gpmd::CDXLRelStats(mp, rel_stats_id, makeMDName(mp, table_id), rel_stats->getRows(), false);
    return IMDCacheObjectRef(rel_stats_obj);
}

IMDCacheObjectRef OrcaMetadataProvider::buildColStatsObj(CMemoryPool * mp, gpmd::CMDIdGPDB * rel_mdid, ColumnStatisticsPtr col_stats, Int32 attrno)
{

    String col_name;
    for (const auto & n : col_stats->name_parts)
    {
        if (!col_name.empty())
            col_name += ".";
        col_name += n;
    }

    rel_mdid->AddRef();
    gpmd::CMDIdColStats * col_stats_id = GPOS_NEW(mp) gpmd::CMDIdColStats(rel_mdid, attrno);
    col_stats_id->AddRef();

    gpdxl::CDXLBucketArray * dxl_stats_bucket_array = GPOS_NEW(mp) gpdxl::CDXLBucketArray(mp);
    dxl_stats_bucket_array->AddRef();
    gpmd::CDXLColStats * col_stats_obj = GPOS_NEW(mp) gpmd::CDXLColStats(mp,
        col_stats_id, /*mdid_col_stats*/
        makeMDName(mp, col_name), /*mdname*/
        CHToOrcaUtil::extracOrcaTypeWidth(col_stats->type), /*width*/
        col_stats->null_freq, /*null_freq*/
        col_stats->ndv_freq, /*distinct_remaining*/
        1.0 - col_stats->null_freq, /*freq_remaining*/
        dxl_stats_bucket_array,
        col_stats->is_stats_missing /*is_col_stats_missing*/);
    return IMDCacheObjectRef(col_stats_obj);
}

OrcaMetadataProviderManager::OrcaMetadataProviderManager()
    : mem_pool(gpos::CAutoMemoryPool::ElcNone)
    , static_part_metadata_retriever(nullptr)
{}

OrcaMetadataProviderManager & OrcaMetadataProviderManager::instance()
{
    static OrcaMetadataProviderManager instance;
    return instance;
}

void OrcaMetadataProviderManager::initOnce(const String &static_metadata_file)
{
    if (has_initialized)
        return;
    std::lock_guard lock(mutex);
    if (has_initialized)
        return;

    //LOG_INFO(logger, "load static metadata from file: {}", static_metadata_file);

    static_part_metadata_retriever = std::make_shared<MetadataObjectRetriever>();

    GPOS_ASSERT(!static_metadata_file.empty())
    CAutoRg<CHAR> dxl_file;
    dxl_file = gpdxl::CDXLUtils::Read(mem_pool.Pmp(), static_metadata_file.c_str());
    CAutoRef<gpdxl::IMDCacheObjectArray> mdcache_obj_array(gpdxl::CDXLUtils::ParseDXLToIMDObjectArray(mem_pool.Pmp(), dxl_file.Rgt(), nullptr /*xsd_file_path*/));

    const ULONG size = mdcache_obj_array->Size();

    // load objects into the hash map
    for (ULONG ul = 0; ul < size; ul++)
    {
        IMDCacheObjectRef obj((*mdcache_obj_array)[ul]);
        static_part_metadata_retriever->addMetadataCacheObject(obj);
    }
}

MemPoolObject<OrcaMetadataProvider> OrcaMetadataProviderManager::getMetadataProvider(ContextPtr context, ASTPtr query)
{
    MemPoolObject<OrcaMetadataProvider> provider(
        GPOS_NEW(mem_pool.Pmp()) OrcaMetadataProvider(mem_pool.Pmp(), static_part_metadata_retriever, context, query));
    return provider;

}

}
