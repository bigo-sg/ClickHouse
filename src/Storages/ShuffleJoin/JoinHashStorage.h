#pragma once
#include <Storages/IStorage.h>
namespace DB
{
class JoinHashStorage : public IStorage
{
public:
    std::string getName() const { return "JoinHashStorage"; }

    Pipe read(const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;
    
    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context) override;
    
};     
}