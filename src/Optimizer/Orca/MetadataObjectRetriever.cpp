#include <Optimizer/Orca/MetadataObjectRetriever.h>
#include <Optimizer/Orca/MetadataUtil.h>
#include <naucrates/md/CMDIdGPDB.h>
#include <naucrates/md/IMDCacheObject.h>
#include <naucrates/md/IMDId.h>
#include <naucrates/md/IMDScalarOp.h>
#include <naucrates/md/IMDType.h>
#include <naucrates/md/IMDCast.h>
#include <naucrates/md/IMDRelation.h>
#include <naucrates/md/IMDCheckConstraint.h>
#include <naucrates/md/IMDScCmp.h>
#include <naucrates/md/IMDFunction.h>
#include <naucrates/md/IMDCast.h>
#include <naucrates/md/IMDColStats.h>
#include <naucrates/md/IMDRelStats.h>
#include <naucrates/md/IMDTrigger.h>
#include <naucrates/md/IMDAggregate.h>
#include <naucrates/md/IMDIndex.h>
#include <sstream>
#include <Common/logger_useful.h>
#include "Optimizer/Orca/OrcaUtil.h"

using IMDCacheObject = gpmd::IMDCacheObject;
using IMDId = gpmd::IMDId;
namespace DB
{
std::map<IMDCacheObject::Emdtype, String> MetadataObjectRetriever::mdtype_key_prefix = {
    {IMDCacheObject::EmdtRel, "__EmdtRel__"},
    {IMDCacheObject::EmdtInd, "__EmdtInd__"},
    {IMDCacheObject::EmdtFunc, "__EmdtFunc__"},
    {IMDCacheObject::EmdtAgg, "__EmdtAgg__"},
    {IMDCacheObject::EmdtOp, "__EmdtOp__"},
    {IMDCacheObject::EmdtType, "__EmdtType__"},
    {IMDCacheObject::EmdtTrigger, "__EmdtTrigger__"},
    {IMDCacheObject::EmdtCheckConstraint, "__EmdtCheckConstraint__"},
    {IMDCacheObject::EmdtRelStats, "__EmdtRelStats__"},
    {IMDCacheObject::EmdtColStats, "__EmdtColStats__"},
    {IMDCacheObject::EmdtCastFunc, "__EmdtCastFunc__"},
    {IMDCacheObject::EmdtScCmp, "__EmdtScCmp__"}
};

MetadataObjectRetriever & MetadataObjectRetriever::operator=(const MetadataObjectRetriever &a)
{
    if (&a != this)
    {
        name_to_objects = a.name_to_objects;
        id_to_objects = a.id_to_objects;
    }
    return *this;
}

bool MetadataObjectRetriever::addMetadataCacheObject(const MemPoolObject<IMDCacheObject> && object_)
{
    const MemPoolObject<IMDCacheObject> & object = object_;
    return addMetadataCacheObject(object);
}

bool MetadataObjectRetriever::addMetadataCacheObject(const MemPoolObject<IMDCacheObject> & object)
{
    if (object.get() == nullptr)
        return false;

    String key;
    auto prefix_iter = mdtype_key_prefix.find(object->MDType());
    if (prefix_iter == mdtype_key_prefix.end())
    {
        LOG_INFO(logger, "Unknow meta data type: {}", static_cast<Int32>(object->MDType()));
        return false;
    }
    auto & key_prefix = prefix_iter->second;
    if (object->MDType() == gpmd::IMDCacheObject::EmdtColStats)
    {
        auto * col_stats_object = dynamic_cast<gpmd::IMDColStats*>(object.get());
        auto * col_id = gpmd::CMDIdColStats::CastMdid(col_stats_object->MDId());
        auto * inner_rel_id = gpmd::CMDIdGPDB::CastMdid(col_id->GetRelMdId());
        auto rel_object = getObjectById(IMDIdRef{inner_rel_id});
        if (rel_object.get() == nullptr)
        {
            LOG_INFO(logger, "add EmdtColStats object failed");
            return false;
        }
        key = key_prefix + getMDCahceObjectName(rel_object) + "." + getMDCahceObjectName(object);
    }
    else if (object->MDType() == gpmd::IMDCacheObject::EmdtOp)
    {
       auto * op_object = dynamic_cast<gpmd::IMDScalarOp*>(object.get());
       IMDIdRef l_id(op_object->GetLeftMdid()), r_id(op_object->GetRightMdid());
       auto l_obj = getObjectById(l_id), r_obj = getObjectById(r_id);
       if (l_obj.get() == nullptr || r_obj.get() == nullptr)
       {
           LOG_INFO(logger, "add EmdtOp object failed");
           return false;
       }
       key = key_prefix + getMDCahceObjectName(object) + "(" + getMDCahceObjectName(l_obj) + "," + getMDCahceObjectName(r_obj) + ")";
    }
    else if (object->MDType() == gpmd::IMDCacheObject::EmdtScCmp)
    {
        auto * sc_cmp_obj = dynamic_cast<gpmd::IMDScCmp *>(object.get());
        IMDIdRef l_id(sc_cmp_obj->GetLeftMdid()), r_id(sc_cmp_obj->GetRightMdid());
        auto l_obj = getObjectById(l_id), r_obj = getObjectById(r_id);
        if (l_obj.get() == nullptr || r_obj.get() == nullptr)
        {
           LOG_INFO(logger, "add EmdtScCmp object failed");
            return false;
        }
        key = key_prefix + getMDCahceObjectName(object) + "(" + getMDCahceObjectName(l_obj) + "," + getMDCahceObjectName(r_obj) + ")";
    }
    else if (object->MDType() == gpmd::IMDCacheObject::EmdtFunc)
    {
        auto * func_obj = dynamic_cast<gpmd::IMDFunction*>(object.get());
        IMDIdRef ret_id(func_obj->GetResultTypeMdid());
        auto ret_obj = getObjectById(ret_id);
        if (ret_obj.get() == nullptr)
        {
           LOG_INFO(logger, "add EmdtFunc object failed");
            return false;
        }
        key = key_prefix + getMDCahceObjectName(object) + "->" + getMDCahceObjectName(ret_obj);
    }
    else
    {
        key = key_prefix + getMDCahceObjectName(object);
    }
    name_to_objects[key] = object;

    IMDIdRef md_id(object->MDId());
    LOG_INFO(logger, "add object: {}", serializedMDId(md_id));
    id_to_objects[serializedMDId(md_id)] = object;
    return true;
}

IMDCacheObjectRef MetadataObjectRetriever::getObjectById(const IMDIdRef & id)
{
    auto key = serializedMDId(id);
    auto iter = id_to_objects.find(key);
    if (iter == id_to_objects.end())
    {
        LOG_INFO(logger, "Not found IMDCacheObject : {}", key);
        return IMDCacheObjectRef(nullptr);
    }
    return iter->second;
}

IMDCacheObjectRef MetadataObjectRetriever::getObjectById(const IMDIdRef && id_)
{
    const IMDIdRef & id = id_;
    return getObjectById(id);
}

/// Get type object by type name
IMDCacheObjectRef MetadataObjectRetriever::getTypeObject(const String & type_name)
{
    String key = mdtype_key_prefix[IMDCacheObject::EmdtType] + type_name;
    auto it = name_to_objects.find(key);
    if (it == name_to_objects.end())
        return IMDCacheObjectRef(nullptr);
    return it->second;
}
/// Get table object by table name
IMDCacheObjectRef MetadataObjectRetriever::getReltaionObject(const String & table)
{
    String key = mdtype_key_prefix[IMDCacheObject::EmdtRel] + table;
    auto it = name_to_objects.find(key);
    if (it == name_to_objects.end())
        return IMDCacheObjectRef(nullptr);
    return it->second;

}

IMDCacheObjectRef MetadataObjectRetriever::getReltaionStatsObject(const String & table)
{
    String key = mdtype_key_prefix[IMDCacheObject::EmdtRelStats] + table;
    auto it = name_to_objects.find(key);
    if (it == name_to_objects.end())
        return IMDCacheObjectRef(nullptr);
    return it->second;

}
IMDCacheObjectRef MetadataObjectRetriever::getColumnStatsObject(const String & table, const String & column)
{
    String key = mdtype_key_prefix[IMDCacheObject::EmdtColStats] + table + "." + column;
    auto it = name_to_objects.find(key);
    if (it == name_to_objects.end())
        return IMDCacheObjectRef(nullptr);
    return it->second;

}
IMDCacheObjectRef MetadataObjectRetriever::getScalarOperatorObject(const String & op_name, const IMDIdRef & left_type_id, const IMDIdRef & right_type_id)
{
    IMDCacheObjectRef l_obj(getObjectById(left_type_id)), r_obj(getObjectById(right_type_id));
    if (l_obj.get() == nullptr || r_obj.get() == nullptr)
    {
        LOG_INFO(logger, "Invalid left({})/right({}) type id", serializedMDId(left_type_id), serializedMDId(right_type_id));
        return IMDCacheObjectRef(nullptr);
    }
    String key = mdtype_key_prefix[IMDCacheObject::EmdtColStats] + op_name + "(" + getMDCahceObjectName(l_obj) + ","
        + getMDCahceObjectName(r_obj) + ")";
    auto it = name_to_objects.find(key);
    if (it == name_to_objects.end())
        return IMDCacheObjectRef(nullptr);
    return it->second;

}
IMDCacheObjectRef MetadataObjectRetriever::getFunctionObject(const String & func, const IMDIdRef & return_type_id)
{
    IMDCacheObjectRef ret_obj(getObjectById(return_type_id));
    if (ret_obj.get() == nullptr)
    {
        LOG_INFO(logger, "Invalid return({}) type id", serializedMDId(return_type_id));
        return IMDCacheObjectRef(nullptr);
    }
    String key = mdtype_key_prefix[IMDCacheObject::EmdtColStats] + func + "->" + getMDCahceObjectName(ret_obj);
    auto it = name_to_objects.find(key);
    if (it == name_to_objects.end())
        return IMDCacheObjectRef(nullptr);
    return it->second;

}

MetadataObjectRetriever::~MetadataObjectRetriever() = default;

}
