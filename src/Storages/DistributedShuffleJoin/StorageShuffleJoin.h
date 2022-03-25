#pragma once
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>
#include <Poco/Logger.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class StorageShuffleJoin : public shared_ptr_helper<StorageShuffleJoin>, public IStorage, WithContext
{
public:
    String getName() const override { return "StorageShuffleJoin"; }
    Pipe read(
        const Names & column_names_,
        const StorageMetadataPtr & metadata_snapshot_,
        SelectQueryInfo & query_info_,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage_,
        size_t max_block_size_,
        unsigned num_streams_) override;

    SinkToStoragePtr write(
        const ASTPtr & ast,
        const StorageMetadataPtr & storage_metadata,
        ContextPtr context
    ) override;

    StorageShuffleJoin(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_);

    #if 0
    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr context_, QueryProcessingStage::Enum to_stage_, const StorageMetadataPtr &, SelectQueryInfo &) const override;
    #endif
private:
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleJoin");
    ASTPtr query;
    String cluster_name;
    String session_id;
    String table_id;
    ASTPtr hash_expr_list;
};

class StorageShuffleJoinPart : public shared_ptr_helper<StorageShuffleJoinPart>, public IStorage, WithContext
{
public:
    String getName() const override { return "StorageShuffleJoinPart"; }
    Pipe read(
        const Names & column_names_,
        const StorageMetadataPtr & metadata_snapshot_,
        SelectQueryInfo & query_info_,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage_,
        size_t max_block_size_,
        unsigned num_streams_) override;

    SinkToStoragePtr write(
        const ASTPtr & ast,
        const StorageMetadataPtr & storage_metadata,
        ContextPtr context
    ) override;

    StorageShuffleJoinPart(
        ContextPtr context_,
        ASTPtr query_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_
    );
private:
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleJoinPart");
    ASTPtr query;
    String session_id;
    String table_id;
};


//void testSinker(ContextPtr context);
}
