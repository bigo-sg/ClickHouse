#pragma once
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>
#include <Poco/Logger.h>
#include "base/types.h"
#include <Storages/SelectQueryInfo.h>

namespace DB
{


class StorageShuffleBase : public IStorage, WithContext
{
public:
    virtual String getName() const override = 0;
    Pipe read(
        const Names & column_names_,
        const StorageSnapshotPtr & metadata_snapshot_,
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

    StorageShuffleBase(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_,
        UInt64 active_sinks_);
    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr local_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & metadata_snapshot,
        SelectQueryInfo & query_info) const override;
protected:
    Poco::Logger * logger;
    ASTPtr query;
    String cluster_name;
    String session_id;
    String table_id;
    ASTPtr hash_expr_list;
    UInt64 active_sinks;

};
class StorageShuffleJoin : public shared_ptr_helper<StorageShuffleJoin>, public StorageShuffleBase
{
public:
    static const String NAME;
    String getName() const override { return "StorageShuffleJoin"; }

    StorageShuffleJoin(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_,
        UInt64 active_sinks_);
};

class StorageShuffleAggregation : public shared_ptr_helper<StorageShuffleJoin>, public StorageShuffleBase
{
public:
    static const String NAME; 
    String getName() const override { return "StorageShuffleAggregation"; }
    StorageShuffleAggregation(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_,
        UInt64 active_sinks_);
};

class StorageShuffleJoinPart : public shared_ptr_helper<StorageShuffleJoinPart>, public IStorage, WithContext
{
public:
    String getName() const override { return "StorageShuffleJoinPart"; }
    Pipe read(
        const Names & column_names_,
        const StorageSnapshotPtr & metadata_snapshot_,
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
        const ColumnsDescription & columns_,
        UInt64 active_sinks_
    );
private:
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleJoinPart");
    ASTPtr query;
    String session_id;
    String table_id;
    UInt64 active_sinks;
};


//void testSinker(ContextPtr context);
}
