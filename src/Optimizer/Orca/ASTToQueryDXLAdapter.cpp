#include <Optimizer/Orca/ASTToQueryDXLAdapter.h>
#include <naucrates/dxl/operators/CDXLColDescr.h>
#include <naucrates/dxl/operators/CDXLColRef.h>
#include <naucrates/dxl/operators/CDXLLogicalGet.h>
#include <naucrates/dxl/operators/CDXLNode.h>
#include <naucrates/dxl/operators/CDXLScalarIdent.h>
#include <naucrates/dxl/operators/CDXLTableDescr.h>
#include <naucrates/md/CMDName.h>
#include <Optimizer/Orca/OrcaUtil.h>
#include <base/types.h>

namespace DB
{
/// refer to parser/CParseHandlerFactory.cpp for how to build cdxl nodes

ASTToQueryDXLAdapter::ASTToQueryDXLAdapter(ContextPtr context_, ASTPtr query_, MetadataObjectRetrieverPtr metadata_retriever_, CMemoryPool * mem_pool_)
    : context(context_)
    , query(query_)
    , metadata_retriever(metadata_retriever_)
    , mem_pool(mem_pool_)
{}

#if 0
ASTToQueryDXLAdapter::Result ASTToQueryDXLAdapter::build(ContextPtr context, ASTPtr query)
{
    return {};
}
#else /// demo test, select * from default.test1
ASTToQueryDXLAdapter::Result ASTToQueryDXLAdapter::build()
{
    result.query = buildTableGet();
    result.cte_producers = buildCTE();
    result.query_output = buildQueryOutput();
    return result;
}

MemPoolObject<gpdxl::CDXLNodeArray> ASTToQueryDXLAdapter::buildQueryOutput()
{
    MemPoolObject<gpdxl::CDXLNodeArray> query_output(GPOS_NEW(mem_pool) gpdxl::CDXLNodeArray(mem_pool));
    
    auto add_output_col = [&](const String & col_name, UInt64 col_id, UInt32 type_id)
    {
        gpmd::CMDName * md_col_name = makeMDName(mem_pool, col_name);
        IMDIdRef md_id = makeMDIdGPDB(mem_pool, type_id);
        md_id.get()->AddRef();
        gpdxl::CDXLColRef * md_col_ref = GPOS_NEW(mem_pool) gpdxl::CDXLColRef(mem_pool, md_col_name, col_id, md_id.get(), -1);
        md_col_ref->AddRef();
        auto * md_ident_op = GPOS_NEW(mem_pool) gpdxl::CDXLScalarIdent(mem_pool, md_col_ref);
        md_ident_op->AddRef();
        gpdxl::CDXLNode * md_ident_node = GPOS_NEW(mem_pool) gpdxl::CDXLNode(mem_pool);
        md_ident_node->SetOperator(md_ident_op);
        md_ident_node->AddRef();
        query_output->Append(md_ident_node);
    };
    add_output_col("a", 1, 23);
    add_output_col("b", 2, 23);
    add_output_col("day", 3, 25);
    return query_output;
}

MemPoolObject<gpdxl::CDXLNodeArray> ASTToQueryDXLAdapter::buildCTE()
{
    MemPoolObject<gpdxl::CDXLNodeArray> cte_array(GPOS_NEW(mem_pool) gpdxl::CDXLNodeArray(mem_pool));
    return {cte_array};
}

MemPoolObject<gpdxl::CDXLNode> ASTToQueryDXLAdapter::buildTableGet()
{
    auto build_col_desc = [&](gpdxl::CDXLColDescrArray * col_array, const String & name, UInt64 col_id, Int32 attrno, UInt32 type_id)
    {
        gpmd::CMDName * md_col_name = makeMDName(mem_pool, name);
        IMDIdRef md_type_id = makeMDIdGPDB(mem_pool, type_id);
        md_type_id->AddRef();
        gpdxl::CDXLColDescr * md_col_desc = GPOS_NEW(mem_pool) gpdxl::CDXLColDescr(mem_pool, md_col_name, col_id, attrno, md_type_id.get(), -1, false, static_cast<UInt64>(-1));
        md_col_desc->AddRef();
        col_array->Append(md_col_desc);
    };
    gpdxl::CDXLColDescrArray * col_desc_array = GPOS_NEW(mem_pool) gpdxl::CDXLColDescrArray(mem_pool);
    build_col_desc(col_desc_array, "a", 1, 1, 23);
    build_col_desc(col_desc_array, "b", 2, 2, 23);
    build_col_desc(col_desc_array, "day", 3, 3, 25);
    col_desc_array->AddRef();

    auto md_table_obj = metadata_retriever->getReltaionObject("default.test1");
    auto * md_table_id = md_table_obj->MDId();
    md_table_id->AddRef();
    auto * md_table_name = makeMDName(mem_pool, "default.test1");
    gpdxl::CDXLTableDescr * table_descr = GPOS_NEW(mem_pool) gpdxl::CDXLTableDescr(mem_pool, md_table_id, md_table_name, 0l);
    table_descr->SetColumnDescriptors(col_desc_array);
    table_descr->AddRef();
    gpdxl::CDXLNode * md_node = GPOS_NEW(mem_pool) gpdxl::CDXLNode(mem_pool, GPOS_NEW(mem_pool) gpdxl::CDXLLogicalGet(mem_pool, table_descr));
    return MemPoolObject<gpdxl::CDXLNode>(md_node);
}

#endif
}
