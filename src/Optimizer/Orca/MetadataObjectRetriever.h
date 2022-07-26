#pragma once
#include <memory>
#include <unordered_map>
#include <naucrates/md/IMDCacheObject.h>
#include <base/types.h>
#include <naucrates/md/IMDId.h>
#include <Poco/Logger.h>
#include <Optimizer/Orca/OrcaUtil.h>
namespace DB
{
class MetadataObjectRetriever
{
public:
    MetadataObjectRetriever() = default;
    ~MetadataObjectRetriever();
    MetadataObjectRetriever & operator = (const MetadataObjectRetriever & a);
    bool addMetadataCacheObject(const MemPoolObject<gpmd::IMDCacheObject> & object);
    bool addMetadataCacheObject(const MemPoolObject<gpmd::IMDCacheObject> && object_);

    /// Get type object by type name
    IMDCacheObjectRef getTypeObject(const String & type_name);
    /// Get table object by table name
    IMDCacheObjectRef getReltaionObject(const String & table);
    IMDCacheObjectRef getReltaionStatsObject(const String & table);
    IMDCacheObjectRef getColumnStatsObject(const String & table, const String & column);
    IMDCacheObjectRef getScalarOperatorObject(const String & op_name, const IMDIdRef & left_type_id, const IMDIdRef & right_type_id);
    IMDCacheObjectRef getFunctionObject(const String & func, const IMDIdRef & return_type_id);

    IMDCacheObjectRef getObjectById(const IMDIdRef & id);
    IMDCacheObjectRef getObjectById(const IMDIdRef && id_);

private:
    static std::map<gpmd::IMDCacheObject::Emdtype, String> mdtype_key_prefix;
    Poco::Logger * logger = &Poco::Logger::get("MetadataObjectRetriever");
    std::unordered_map<String, IMDCacheObjectRef> name_to_objects;
    std::unordered_map<String, IMDCacheObjectRef> id_to_objects;


    static String buildObjectIndexKey(const MemPoolObject<gpmd::IMDCacheObject> & object);
    static String serializeMDId(const MemPoolObject<gpmd::IMDId> & id);
};
using MetadataObjectRetrieverPtr = std::shared_ptr<MetadataObjectRetriever>;
}
