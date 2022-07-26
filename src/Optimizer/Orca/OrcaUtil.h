#pragma once
#include <string>
#include <base/types.h>
#include <gpos/memory/CMemoryPool.h>
#include <naucrates/dxl/operators/CDXLNode.h>
#include <naucrates/md/IMDCacheObject.h>
#include <naucrates/md/IMDId.h>
#include <Optimizer/Orca/MetadataUtil.h>
#include <naucrates/md/CMDIdGPDB.h>
#include <naucrates/md/CMDIdColStats.h>
#include <naucrates/md/CMDIdRelStats.h>
#include <naucrates/md/CMDIdCast.h>
#include <naucrates/md/CMDIdScCmp.h>
#include <naucrates/md/CMDIdGPDBCtas.h>
#include <naucrates/md/IMDId.h>

namespace DB
{
template<class T>
class MemPoolObject
{
public:
    MemPoolObject() : object(nullptr) {}
    explicit MemPoolObject(T * object_) : object(object_)
    {
        if (object)
            object->AddRef();
    }

    MemPoolObject(const MemPoolObject<T> & a) : object(a.object)
    {
        if (object)
            object->AddRef();
    }

    MemPoolObject & operator = (const MemPoolObject<T> & a)
    {
        if (&a != this)
        {
            if (object)
                object->Release();
            object = a.object;
            if (object)
                object->AddRef();
        }
        return *this;
    } 

    ~MemPoolObject()
    {
        if (object)
            object->Release();
    }

    T * get() const { return object; }

    T * operator -> () const { return object; }

private:
    mutable T * object;

};
using IMDCacheObjectRef = MemPoolObject<gpmd::IMDCacheObject>;
using IMDIdRef = MemPoolObject<gpmd::IMDId>;

String convertWStringToString(const std::wstring & wstr);

String serializedMDId(const IMDIdRef & id, bool use_md_type = true);
String serializedMDId(const IMDIdRef && id_);
String serializeMDObj(const gpmd::IMDCacheObject * obj, bool serialize_document_header_footer = true, bool indentation = true);
String serializePlan(const gpdxl::CDXLNode * plan);
String serializeQuery(const gpdxl::CDXLNode * query, const gpdxl::CDXLNodeArray * query_output_dxlnode_array, const gpdxl::CDXLNodeArray * cte_producers);

IMDIdRef makeMDIdGPDB(CMemoryPool *mp, UInt64 id);

gpmd::CMDName * makeMDName(CMemoryPool * mp, const String & name);

String getMDCahceObjectName(const IMDCacheObjectRef & object);

}
