#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <base/types.h>
#include <Poco/Logger.h>

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
        ASTPtr hash_expr_list_);
    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr local_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & metadata_snapshot,
        SelectQueryInfo & query_info) const override;

    ///
    /// Do not set it true until we find other a way to signal the table has finished sinking
    ///
    bool supportsParallelInsert() const override { return false; }
protected:
    Poco::Logger * logger;
    ASTPtr query;
    String cluster_name;
    String session_id;
    String table_id;
    ASTPtr hash_expr_list;
};

///
/// StorageShuffleJoin and StorageShuffleAggregation have the same behavior. the only difference is the name
/// for some special usage purpose.
///
class StorageShuffleJoin : public StorageShuffleBase
{
public:
    static constexpr auto NAME = "StorageShuffleJoin";
    String getName() const override { return NAME; }

    StorageShuffleJoin(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_);
};

class StorageShuffleAggregation : public StorageShuffleBase
{
public:
    static constexpr auto NAME = "StorageShuffleAggregation";
    String getName() const override { return NAME; }
    StorageShuffleAggregation(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_);
};

///
/// use for reading/writing the local ShuffleBlockTable
///

class StorageLocalShuffle : public IStorage, WithContext
{
public:
    static constexpr auto NAME = "StorageLocalShuffle";
    String getName() const override { return NAME; }
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

    StorageLocalShuffle(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_
    );

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr local_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & metadata_snapshot,
        SelectQueryInfo & query_info) const override;
private:
    Poco::Logger * logger = &Poco::Logger::get("StorageLocalShuffle");
    ASTPtr query;
    String cluster_name;
    String session_id;
    String table_id;
};


///
/// Use to close a shuffle table
///

class StorageShuffleClose : public IStorage, WithContext
{
public:
    static constexpr auto NAME = "StorageShuffleClose";
    String getName() const override { return NAME; }
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
        ContextPtr context_) override;

    StorageShuffleClose(
        ContextPtr context_,
        ASTPtr query_,
        const ColumnsDescription & columns_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_);

private:
    ASTPtr query;
    String cluster_name;
    String session_id;
    String table_id;

};

}
