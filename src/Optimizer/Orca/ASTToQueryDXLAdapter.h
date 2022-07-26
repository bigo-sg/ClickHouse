#pragma once
#include <gpos/memory/CMemoryPool.h>
#include <naucrates/dxl/operators/CDXLNode.h>
#include <Optimizer/Orca/OrcaUtil.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Poco/Logger.h>
#include "base/types.h"
#include <Optimizer/Orca/MetadataObjectRetriever.h>
namespace DB
{
/// Translate a CH AST into Orca DXL Nodes
class ASTToQueryDXLAdapter
{
public:
    struct Result
    {
        MemPoolObject<gpdxl::CDXLNodeArray> query_output;
        MemPoolObject<gpdxl::CDXLNode> query;
        MemPoolObject<gpdxl::CDXLNodeArray> cte_producers;
    };

    explicit ASTToQueryDXLAdapter(ContextPtr context_, ASTPtr query_, MetadataObjectRetrieverPtr metadata_retriever_, CMemoryPool * mem_pool_);
    ~ASTToQueryDXLAdapter() = default;

    Result build();

private:
    Poco::Logger * logger = &Poco::Logger::get("ASTToQueryDXLAdapter");
    ContextPtr context;
    ASTPtr query;
    MetadataObjectRetrieverPtr metadata_retriever;
    CMemoryPool * mem_pool;

    //UInt64 col_id_assigned = 0l;

    Result result;

    MemPoolObject<gpdxl::CDXLNodeArray> buildCTE();
    MemPoolObject<gpdxl::CDXLNodeArray> buildQueryOutput();

    MemPoolObject<gpdxl::CDXLNode> buildTableGet();
};
}
