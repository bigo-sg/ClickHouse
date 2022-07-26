#include <Optimizer/Orca/OrcaUtil.h>
#include <locale>
#include <codecvt>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <gpos/common/CAutoP.h>
#include <gpos/memory/CMemoryPool.h>
#include <gpos/memory/CAutoMemoryPool.h>
#include <naucrates/md/CMDIdScCmp.h>
#include <naucrates/md/IMDId.h>
#include <naucrates/dxl/CDXLUtils.h>
#include <IO/WriteHelpers.h>
#include <gpos/string/CWStringDynamic.h>

namespace DB
{
String convertWStringToString(const std::wstring & wstr)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> converter;
    return converter.to_bytes(wstr);
}

String serializedMDId(const IMDIdRef & id, bool use_md_type)
{
    WriteBufferFromOwnString stream;
    auto id_type = id->MdidType();
    if (use_md_type)
        stream << static_cast<Int32>(id->MdidType()) << "."; 
    if (id_type == gpmd::IMDId::EmdidGPDB || id_type == gpmd::IMDId::EmdidGPDBCtas)
    {
        auto * gpdb_id = dynamic_cast<gpmd::CMDIdGPDB*>(id.get());
        stream << gpdb_id->Oid() << "." << gpdb_id->VersionMajor() << "." << gpdb_id->VersionMinor();
    }
    else if (id_type == gpmd::IMDId::EmdidColStats)
    {
        auto * col_id = gpmd::CMDIdColStats::CastMdid(id.get());
        IMDIdRef rel_id(col_id->GetRelMdId());
        stream << serializedMDId(rel_id, false) << "." << col_id->Position();
    }
    else if (id_type == gpmd::IMDId::EmdidRelStats)
    {
        auto * rel_id = gpmd::CMDIdRelStats::CastMdid(id.get());
        IMDIdRef md_id(rel_id->GetRelMdId());
        stream << serializedMDId(md_id, false);
    }
    else if (id_type == gpmd::IMDId::EmdidCastFunc)
    {
        auto * cast_id = gpmd::CMDIdCast::CastMdid(id.get());
        IMDIdRef src_id(cast_id->MdidSrc()), dest_id(cast_id->MdidDest());
        stream << serializedMDId(src_id, false) << ";" << serializedMDId(dest_id, false);
    }
    else if (id_type == gpmd::IMDId::EmdidScCmp)
    {
        auto * sc_id = gpmd::CMDIdScCmp::CastMdid(id.get());
        IMDIdRef left_id(sc_id->GetLeftMdid()), right_id(sc_id->GetRightMdid());
        stream << serializedMDId(left_id, false) << ";" << serializedMDId(right_id, false) << ";" << sc_id->ParseCmpType();
    }

    return stream.str();
}

String serializedMDId(const IMDIdRef && id_)
{
    const IMDIdRef & id = id_;
    return serializedMDId(id);
}

String serializeMDObj(const gpmd::IMDCacheObject * obj, bool serialize_document_header_footer, bool indentation)
{
    gpos::CAutoMemoryPool mem_pool(gpos::CAutoMemoryPool::ElcNone);
    auto * buffer = gpdxl::CDXLUtils::SerializeMDObj(mem_pool.Pmp(), obj, serialize_document_header_footer, indentation);
    auto resutl = convertWStringToString(buffer->GetBuffer());
    GPOS_DELETE(buffer);
    return resutl;
}

String serializePlan(const gpdxl::CDXLNode * plan)
{
    gpos::CAutoMemoryPool mem_pool(gpos::CAutoMemoryPool::ElcNone);
    CMemoryPool * mp = mem_pool.Pmp();
    CWStringDynamic buffer(mp);
    COstreamString ss(&buffer);
    gpdxl::CDXLUtils::SerializePlan(mp, ss, plan, 0, 4, true, true);
    return convertWStringToString(buffer.GetBuffer());
}

String serializeQuery(const gpdxl::CDXLNode * query, const gpdxl::CDXLNodeArray * query_output_dxlnode_array, const gpdxl::CDXLNodeArray * cte_producers)
{
    gpos::CAutoMemoryPool mem_pool(gpos::CAutoMemoryPool::ElcNone);
    CMemoryPool * mp = mem_pool.Pmp();
    CWStringDynamic buffer(mp);
    COstreamString ss(&buffer);
    gpdxl::CDXLUtils::SerializeQuery(mp, ss, query, query_output_dxlnode_array, cte_producers, true, true);
    return convertWStringToString(buffer.GetBuffer());
}

String getMDCahceObjectName(const IMDCacheObjectRef & object)
{
    return convertWStringToString(object->Mdname().GetMDName()->GetBuffer());
}

IMDIdRef makeMDIdGPDB(CMemoryPool *mp, UInt64 id)
{
    auto * md_id = GPOS_NEW(mp) gpdxl::CMDIdGPDB(id, 1, 0);
    return IMDIdRef(md_id);
}

gpmd::CMDName * makeMDName(CMemoryPool * mp, const String & name)
{
    gpos::CWStringDynamic * wstr = GPOS_NEW(mp) gpos::CWStringDynamic(mp);
    wstr->AppendFormat(GPOS_WSZ_LIT("%s"), name.c_str());
    auto * md_name = GPOS_NEW(mp) gpmd::CMDName(mp, wstr);
    GPOS_DELETE(wstr);
    return md_name;
}
}
