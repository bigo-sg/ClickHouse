#pragma once

#include <optional>
#include <unordered_map>
#include <boost/core/noncopyable.hpp>
#include <gpos/base.h>
#include <gpos/common/CHashMap.h>
#include <gpos/memory/CAutoMemoryPool.h>
#include <gpos/memory/CMemoryPool.h>
#include <gpos/string/CWStringDynamic.h>
#include <naucrates/md/IMDCacheObject.h>
#include <naucrates/md/IMDId.h>
#include <naucrates/md/IMDProvider.h>
#include <naucrates/md/CMDColumn.h>
#include <naucrates/md/IMDRelation.h>
#include <base/types.h>

#include <Optimizer/Orca/MetadataObjectRetriever.h>
#include <Poco/Logger.h>
#include <Optimizer/Orca/CatalogIdManager.h>
#include <Storages/IStorage.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Orca/OrcaUtil.h>
#include <Optimizer/Stats/StatisticsData.h>
namespace DB
{
/// build a CMDProviderMemory
class OrcaMetadataProvider : public gpdxl::IMDProvider
{
public:
    using CMemoryPool = gpos::CMemoryPool;
    using CMDAccessor = gpdxl::CMDAccessor;
    using IMDId = gpmd::IMDId;
    using gposCWStringDynamic = gpos::CWStringDynamic;
    using IMDType = gpmd::IMDType;
    using CSystemId = gpdxl::CSystemId;

    using MetadataObjectMap = std::unordered_map<UInt64, gpmd::IMDCacheObject *>;
    explicit OrcaMetadataProvider(
        CMemoryPool * mem_pool, MetadataObjectRetrieverPtr static_metadata_retriver, ContextPtr context, ASTPtr query);
    ~OrcaMetadataProvider() override;


    gpos::CWStringBase * GetMDObjDXLStr(CMemoryPool *mem_pool, CMDAccessor * md_accessor, IMDId *mdid) const override;
    gpmd::IMDId * MDId(CMemoryPool * mp, CSystemId sysid, gpmd::IMDType::ETypeInfo type_info) const override;

    MetadataObjectRetrieverPtr getMetadataObjectRetriever() { return object_retriever; }

private:
    Poco::Logger * logger = &Poco::Logger::get("OrcaMetadataProvider");
    CatalogIdManager id_allocator;
    MetadataObjectRetrieverPtr object_retriever;
    using RelObjectBuilder = std::function<IMDCacheObjectRef(CMemoryPool *, OrcaMetadataProvider *, TableStatisticsPtr)>;
    static std::map<String, RelObjectBuilder> rel_obj_builders;
    static void initRelObjBuilders();
    static std::optional<RelObjectBuilder> getRelObjBuilder(StoragePtr storage);
    static IMDCacheObjectRef buildRelStatsObj(CMemoryPool * mp, gpmd::CMDIdGPDB * rel_mdid, TableStatisticsPtr rel_stats);
    static IMDCacheObjectRef buildColStatsObj(CMemoryPool * mp, gpmd::CMDIdGPDB * rel_mdid, ColumnStatisticsPtr col_stats, Int32 attrno);
    static gpmd::CMDColumn * buildColObj(CMemoryPool * mp, const Strings & names, Int32 attrnum, gpmd::IMDId * md_type_id, bool is_nullable, Int32 length);

    void loadDynamicMetadataObjects(CMemoryPool * mem_pool, ContextPtr context, ASTPtr query);

};


class OrcaMetadataProviderManager : public boost::noncopyable
{
public:
    
    static OrcaMetadataProviderManager & instance();
    void initOnce(const String & static_metadata_file);
    MemPoolObject<OrcaMetadataProvider> getMetadataProvider(ContextPtr context, ASTPtr query);

protected:
    OrcaMetadataProviderManager();

private:
    Poco::Logger * logger = &Poco::Logger::get("OrcaMetadataProviderManager");

    const static UInt64 update_internal_ts = 120;
    std::mutex mutex;
    std::atomic<bool> has_initialized = false;
    CAutoMemoryPool mem_pool;
    MetadataObjectRetrieverPtr static_part_metadata_retriever;
};
}
