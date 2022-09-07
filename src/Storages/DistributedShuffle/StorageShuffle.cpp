#include <algorithm>
#include <condition_variable>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <Columns/FilterDescription.h>
#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/createBlockSelector.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Chunk.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/ISource.h>
#include <QueryPipeline/RemoteInserter.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/DistributedShuffle/ShuffleBlockTable.h>
#include <Storages/DistributedShuffle/StorageShuffle.h>
#include <TableFunctions/TableFunctionShuffle.h>
#include <base/defines.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <llvm/ProfileData/InstrProf.h>
#include <sys/time.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/WeakHash.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
class StorageShuffleSource : public ISource, WithContext
{
public:
    StorageShuffleSource(ContextPtr context_, const String & session_id_, const String & table_id_, const Block & header_)
        : ISource(header_), WithContext(context_), session_id(session_id_), table_id(table_id_), header(header_)
    {
    }

    ~StorageShuffleSource() override
    {
        if (table)
        {
            session->value().releaseTable(table_id);
        }
    }

    String getName() const override { return "StorageShuffleSource"; }
    Chunk generate() override
    {
        tryInitialize();
        if (!table) [[unlikely]]
        {
            LOG_INFO(logger, "{}.{} is not found.", session_id, table_id);
            return {};
        }
        Chunk res = table->popChunk();
        read_rows += res.getNumRows();
        return res;
    }

private:
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleSource");
    bool has_initialized = false;
    String session_id;
    String table_id;
    Block header;
    std::shared_ptr<ShuffleBlockSessionHolder> session;
    ShuffleBlockTablePtr table;
    size_t read_rows = 0;

    void tryInitialize()
    {
        if (likely(has_initialized))
            return;
        session = ShuffleBlockTableManager::getInstance().getOrSetSession(session_id, getContext());
        if (session)
        {
            table = session->value().getTable(table_id, true);
            if (!table)
            {
                LOG_TRACE(logger, "Not found table:{}-{}", session_id, table_id);
            }
        }
        else
        {
            LOG_TRACE(logger, "Not found session:{}", session_id);
        }

        has_initialized = true;
    }
};

class StorageShuffleSink : public SinkToStorage
{
public:
    explicit StorageShuffleSink(
        ContextPtr context_,
        const String & cluster_,
        const String & session_id_,
        const String & table_id_,
        const Block & header_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_)
        : SinkToStorage(header_)
        , context(context_)
        , cluster(cluster_)
        , session_id(session_id_)
        , table_id(table_id_)
        , columns_desc(columns_)
        , hash_expr_list(hash_expr_list_)
    {
    }

    void onFinish() override
    {
        if (has_initialized)
        {
            {
                std::unique_lock chunk_lock(pending_blocks_mutex);
                consume_finished = true;
            }
            pending_blocks_cond.notify_all();
            sink_thread_pool->wait();

            for (auto & inserter : inserters)
            {
                std::lock_guard lock(inserter->mutex);
                inserter->flush();
                inserter->inserter->onFinish();
            }
        }
        if (watch)
        {
            watch->stop();
            size_t elapse = watch->elapsedMilliseconds();
            LOG_INFO(logger, "{}.{} sink elapsed. {}", session_id, table_id, elapse);
        }
    }
    String getName() const override { return "StorageShuffleSink"; }

protected:
    // split the block into multi blocks, and send to different nodes
    void consume(Chunk chunk) override
    {
        if (!has_initialized) [[unlikely]]
        {
            initOnce();
        }
        {
            std::lock_guard lock(pending_blocks_mutex);
            pending_blocks.emplace_back(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
        }
        pending_blocks_cond.notify_one();
    }

private:
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleSink");
    ContextPtr context;
    String cluster;
    String session_id;
    String table_id;
    ColumnsDescription columns_desc;
    ASTPtr hash_expr_list;
    Strings hash_expr_columns_names;

    std::mutex init_mutex;
    std::atomic<bool> has_initialized = false;

    std::atomic<bool> consume_finished = false;
    std::mutex pending_blocks_mutex;
    std::condition_variable pending_blocks_cond;
    std::list<Block> pending_blocks;
    std::unique_ptr<ThreadPool> sink_thread_pool;


    struct InternalInserter
    {
        std::mutex mutex;
        Blocks pending_blocks;
        size_t current_rows = 0;
        size_t max_rows_limit = DEFAULT_BLOCK_SIZE;
        std::shared_ptr<RemoteInserter> inserter;
        void tryWrite(const Block & block)
        {
            if (current_rows + block.rows() > max_rows_limit && !pending_blocks.empty())
            {
                auto to_send_block = concatenateBlocks(pending_blocks);
                inserter->write(to_send_block);
                pending_blocks.clear();
                current_rows = 0;
            }
            current_rows += block.rows();
            pending_blocks.push_back(block);
        }

        void flush()
        {
            if (!pending_blocks.empty())
            {
                auto to_send_block = concatenateBlocks(pending_blocks);
                inserter->write(to_send_block);
                current_rows = 0;
                pending_blocks.clear();
            }
        }
    };
    using InternalInserterPtr = std::shared_ptr<InternalInserter>;
    std::vector<InternalInserterPtr> inserters;
    const static size_t inserter_group_size = 4;
    std::vector<std::shared_ptr<Connection>> node_connections;
    std::shared_ptr<ExpressionActions> hash_expr_cols_actions;
    String hash_expr_column_name;

    std::unique_ptr<Stopwatch> watch;


    void initOnce()
    {
        watch = std::make_unique<Stopwatch>();
        std::lock_guard lock(init_mutex);
        if (has_initialized)
            return;
        initInserters();
        initHashColumnsNames();

        size_t thread_size = context->getSettings().max_threads;
        //size_t thread_size = inserters.size();
        sink_thread_pool = std::make_unique<ThreadPool>(thread_size);
        for (size_t i = 0; i < thread_size; ++i)
        {
            sink_thread_pool->scheduleOrThrowOnError(
                [&]
                {
                    while (true)
                    {
                        Block block;
                        {
                            std::unique_lock chunk_lock(pending_blocks_mutex);
                            if (pending_blocks.empty() && !consume_finished)
                            {
                                pending_blocks_cond.wait(chunk_lock, [&] { return consume_finished || !pending_blocks.empty(); });
                            }
                            if (!pending_blocks.empty())
                            {
                                block.swap(pending_blocks.front());
                                pending_blocks.pop_front();
                            }
                            else if (consume_finished)
                            {
                                break;
                            }
                        }
                        if (block.rows())
                        {
                            std::vector<Block> split_blocks;
                            splitBlock(block, split_blocks);
                            sendBlocks(split_blocks);
                        }
                    }
                });
        }
        has_initialized = true;
    }

    void initInserters()
    {
        // prepare insert sql
        String insert_sql;
        auto names_and_types = columns_desc.getAllPhysical();
        WriteBufferFromOwnString write_buf;
        auto names = names_and_types.getNames();
        auto types = names_and_types.getTypes();
        for (size_t i = 0; i < names.size(); ++i)
        {
            if (i)
                write_buf << ",";
            write_buf << names[i] << " " << types[i]->getName();
        }
        insert_sql = fmt::format(
            "INSERT INTO FUNCTION {}('{}', '{}', '{}', '{}') VALUES",
            TableFunctionLocalShuffle::name,
            cluster,
            session_id,
            table_id,
            write_buf.str());

        const auto & settings = context->getSettings();
        // prepare remote call
        auto cluster_addresses = getSortedShardAddresses();
        for (const auto & node : cluster_addresses)
        {
            #if 0
            auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "StorageShuffleSink",
                node.compression,
                node.secure);
            node_connections.emplace_back(connection);
            auto inserter = std::make_shared<RemoteInserter>(
                *connection,
                ConnectionTimeouts{
                    settings.connect_timeout.value.seconds() * 1000,
                    settings.send_timeout.value.seconds() * 1000,
                    settings.receive_timeout.value.seconds() * 1000},
                insert_sql,
                context->getSettings(),
                context->getClientInfo());
            auto internal_inserter = std::make_shared<InternalInserter>();
            internal_inserter->inserter = inserter;
            internal_inserter->max_rows_limit = context->getSettingsRef().max_block_size;
            inserters.emplace_back(internal_inserter);
            #else
            for (size_t i = 0; i < inserter_group_size; ++i)
            {
                auto connection = std::make_shared<Connection>(
                    node.host_name,
                    node.port,
                    context->getGlobalContext()->getCurrentDatabase(),
                    node.user,
                    node.password,
                    node.cluster,
                    node.cluster_secret,
                    "StorageShuffleSink",
                    node.compression,
                    node.secure);
                node_connections.emplace_back(connection);
                auto inserter = std::make_shared<RemoteInserter>(
                    *connection,
                    ConnectionTimeouts{
                        settings.connect_timeout.value.seconds() * 1000,
                        settings.send_timeout.value.seconds() * 1000,
                        settings.receive_timeout.value.seconds() * 1000},
                    insert_sql,
                    context->getSettings(),
                    context->getClientInfo());
                auto internal_inserter = std::make_shared<InternalInserter>();
                internal_inserter->inserter = inserter;
                internal_inserter->max_rows_limit = context->getSettingsRef().max_block_size;
                inserters.emplace_back(internal_inserter);
            }
            #endif
        }
    }

    void initHashColumnsNames()
    {
        //hash_expr_columns_names
        for (auto & child : hash_expr_list->children)
        {
            hash_expr_columns_names.emplace_back(queryToString(child));
        }
    }

    Cluster::Addresses getSortedShardAddresses() const
    {
        auto cluster_instance = context->getCluster(cluster)->getClusterWithReplicasAsShards(context->getSettings());
        Cluster::Addresses addresses;
        for (const auto & replicas : cluster_instance->getShardsAddresses())
        {
            for (const auto & node : replicas)
            {
                addresses.emplace_back(node);
            }
        }
        std::sort(
            std::begin(addresses),
            std::end(addresses),
            [](const Cluster::Address & a, const Cluster::Address & b) { return a.host_name > b.host_name && a.port > b.port; });
        return addresses;
    }

    static IColumn::Selector hashToSelector(const WeakHash32 & hash, size_t num_shards)
    {
        const auto & data = hash.getData();
        size_t num_rows = data.size();

        IColumn::Selector selector(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
            selector[i] = data[i] % num_shards;
        return selector;
    }

    void splitBlock(Block & original_block, std::vector<Block> & split_blocks)
    {
        size_t num_rows = original_block.rows();
        size_t num_shards = inserters.size() / inserter_group_size;
        Block header = original_block.cloneEmpty();
        ColumnRawPtrs hash_cols;
        for (const auto & hash_col_name : hash_expr_columns_names)
        {
            hash_cols.push_back(original_block.getByName(hash_col_name).column.get());
        }

        WeakHash32 hash(num_rows);
        for (const auto & hash_col : hash_cols)
        {
            hash_col->updateWeakHash32(hash);
        }
        auto selector = hashToSelector(hash, num_shards);

        for (size_t i = 0; i < num_shards; ++i)
        {
            split_blocks.emplace_back(original_block.cloneEmpty());
        }

        auto columns_in_block = header.columns();
        for (size_t i = 0; i < columns_in_block; ++i)
        {
            auto split_columns = original_block.getByPosition(i).column->scatter(num_shards, selector);
            for (size_t block_index = 0; block_index < num_shards; ++block_index)
            {
                split_blocks[block_index].getByPosition(i).column = std::move(split_columns[block_index]);
            }
        }
    }

    void sendBlocks(std::vector<Block> & blocks)
    {
        #if 0
        for (size_t i = 0; i < blocks.size(); ++i)
        {
            auto & block = blocks[i];
            auto & inserter = inserters[i];
            std::lock_guard lock(inserter->mutex);
            inserter->tryWrite(block);
        }
        #else
        static std::atomic<size_t> inserter_round_idx = 0;
        inserter_round_idx += 1;
        size_t idx = inserter_round_idx % inserter_group_size;
        for (size_t i = 0; i < blocks.size(); ++i)
        {
            auto & block = blocks[i];
            auto inserter = inserters[i * inserter_group_size + idx];
            std::lock_guard lock(inserter->mutex);
            inserter->tryWrite(block);
        }
        #endif
    }
};

class StorageLocalShuffleSink : public SinkToStorage
{
public:
    explicit StorageLocalShuffleSink(ContextPtr context_, const String & session_id_, const String & table_id_, const Block & header_)
        : SinkToStorage(header_), context(context_), session_id(session_id_), table_id(table_id_)
    {
        session = ShuffleBlockTableManager::getInstance().getOrSetSession(session_id_, context_);
        if (!session)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Get session({}) storage failed.", session_id_);
        table_storage = session->value().getOrSetTable(table_id_, header_);
        if (!table_storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Get session table({}-{}) failed.", session_id_, table_id_);
    }

    void onFinish() override { LOG_INFO(logger, "{}.{} sink elapsed:{}", session_id, table_id, watch.elapsedMilliseconds()); }
    String getName() const override { return "StorageLocalShuffleSink"; }

protected:
    void consume(Chunk chunk) override { table_storage->addChunk(std::move(chunk)); }

private:
    ContextPtr context;
    String session_id;
    String table_id;
    std::shared_ptr<ShuffleBlockSessionHolder> session;
    ShuffleBlockTablePtr table_storage;
    Poco::Logger * logger = &Poco::Logger::get("StorageLocalShuffleSink");
    Stopwatch watch;
};

///
/// FIXEDME: Maybe need the initiator pass the cluster nodes, not query by the worker nodes. Since the cluster nodes set may change
///
StorageShuffleBase::StorageShuffleBase(
    ContextPtr context_,
    ASTPtr query_,
    const String & cluster_name_,
    const String & session_id_,
    const String & table_id_,
    const ColumnsDescription & columns_,
    ASTPtr hash_expr_list_)
    : IStorage(StorageID(session_id_, table_id_))
    , WithContext(context_)
    , query(query_)
    , cluster_name(cluster_name_)
    , session_id(session_id_)
    , table_id(table_id_)
    , hash_expr_list(hash_expr_list_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageShuffleBase::read(
    const Names & column_names_,
    const StorageSnapshotPtr & metadata_snapshot_,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage_,
    size_t /*max_block_size_*/,
    unsigned num_streams)
{
    auto header = getInMemoryMetadata().getSampleBlock();

    auto query_kind = context_->getClientInfo().query_kind;
    if (query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
    {
        auto source = std::make_shared<StorageShuffleSource>(context_, session_id, table_id, header);
        Pipe res(source);
        res.resize(num_streams);
        return res;
    }
    /// Since the query_info_.query has been rewritten, it may cause an ambiguous column exception in join case.
    /// So we use the original_query here.
    auto remote_query = queryToString(query_info_.original_query);
    auto cluster = context_->getCluster(cluster_name)->getClusterWithReplicasAsShards(context_->getSettings());
    const Scalars & scalars = context_->hasQueryContext() ? context_->getQueryContext()->getScalars() : Scalars{};
    header = InterpreterSelectQuery(query_info_.query, context_, SelectQueryOptions(processed_stage_).analyze()).getSampleBlock();
    Pipes pipes;
    for (const auto & replicas : cluster->getShardsAddresses())
    {
        for (const auto & node : replicas)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context_->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "StorageShuffleBase",
                node.compression,
                node.secure);

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection, remote_query, header, context_, nullptr, scalars, Tables(), processed_stage_, RemoteQueryExecutor::Extension{});
            LOG_TRACE(logger, "run query on node:{}. query:{}", node.host_name, remote_query);
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false));
        }
    }
    metadata_snapshot_->check(column_names_);
    auto res = Pipe::unitePipes(std::move(pipes));
    res.resize(num_streams);
    return res;
}

SinkToStoragePtr StorageShuffleBase::write(const ASTPtr & /*ast*/, const StorageMetadataPtr & /*storage_metadata*/, ContextPtr context_)
{
    // If there is no hash expression, just move blocks into local the shuffle block table
    SinkToStoragePtr sinker;
    if (hash_expr_list)
        sinker = std::make_shared<StorageShuffleSink>(
            context_,
            cluster_name,
            session_id,
            table_id,
            getInMemoryMetadata().getSampleBlock(),
            getInMemoryMetadata().getColumns(),
            hash_expr_list);
    else
        sinker = std::make_shared<StorageLocalShuffleSink>(context_, session_id, table_id, getInMemoryMetadata().getSampleBlock());
    return sinker;
}

QueryProcessingStage::Enum StorageShuffleBase::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info) const
{
    if (local_context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        /// When there is join in the query, cannot enable the two phases processing. It will cause
        /// a column missing exception, if the result column is in the right table but not in the left table
        auto select_query = query_info.query->as<ASTSelectQuery &>();
        if (select_query.join())
            return QueryProcessingStage::FetchColumns;

        if (to_stage >= QueryProcessingStage::WithMergeableState)
            return QueryProcessingStage::WithMergeableState;
    }

    return QueryProcessingStage::FetchColumns;
}

StorageShuffleJoin::StorageShuffleJoin(
    ContextPtr context_,
    ASTPtr query_,
    const String & cluster_name_,
    const String & session_id_,
    const String & table_id_,
    const ColumnsDescription & columns_,
    ASTPtr hash_expr_list_)
    : StorageShuffleBase(context_, query_, cluster_name_, session_id_, table_id_, columns_, hash_expr_list_)
{
    logger = &Poco::Logger::get("StorageShuffleJoin");
}


StorageShuffleAggregation::StorageShuffleAggregation(
    ContextPtr context_,
    ASTPtr query_,
    const String & cluster_name_,
    const String & session_id_,
    const String & table_id_,
    const ColumnsDescription & columns_,
    ASTPtr hash_expr_list_)
    : StorageShuffleBase(context_, query_, cluster_name_, session_id_, table_id_, columns_, hash_expr_list_)
{
    logger = &Poco::Logger::get("StorageShuffleAggregation");
}


StorageLocalShuffle::StorageLocalShuffle(
    ContextPtr context_,
    ASTPtr query_,
    const String & cluster_name_,
    const String & session_id_,
    const String & table_id_,
    const ColumnsDescription & columns_)
    : IStorage(StorageID(session_id_, table_id_ + "_part"))
    , WithContext(context_)
    , query(query_)
    , cluster_name(cluster_name_)
    , session_id(session_id_)
    , table_id(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageLocalShuffle::read(
    const Names & column_names_,
    const StorageSnapshotPtr & metadata_snapshot_,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage_,
    size_t /*max_block_size_*/,
    unsigned /*num_streams_*/)
{
    auto header = getInMemoryMetadata().getSampleBlock();
    auto query_kind = context_->getClientInfo().query_kind;
    LOG_TRACE(logger, "query kind={}, query={}", query_kind, queryToString(query_info_.query));
    if (query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
    {
        return Pipe(std::make_shared<StorageShuffleSource>(context_, session_id, table_id, header));
    }
    auto remote_query = queryToString(query_info_.original_query);
    auto cluster = context_->getCluster(cluster_name)->getClusterWithReplicasAsShards(context_->getSettings());
    const Scalars & scalars = context_->hasQueryContext() ? context_->getQueryContext()->getScalars() : Scalars{};
    header = InterpreterSelectQuery(query_info_.query, context_, SelectQueryOptions(processed_stage_).analyze()).getSampleBlock();
    Pipes pipes;
    for (const auto & replicas : cluster->getShardsAddresses())
    {
        for (const auto & node : replicas)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context_->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "StorageLocalShuffle",
                node.compression,
                node.secure);

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection, remote_query, header, context_, nullptr, scalars, Tables(), processed_stage_, RemoteQueryExecutor::Extension{});
            LOG_TRACE(logger, "run query on node:{}. query:{}", node.host_name, remote_query);
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false));
        }
    }
    metadata_snapshot_->check(column_names_);
    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageLocalShuffle::write(const ASTPtr & /*ast*/, const StorageMetadataPtr & /*storage_metadata*/, ContextPtr context_)
{
    auto sinker = std::make_shared<StorageLocalShuffleSink>(context_, session_id, table_id, getInMemoryMetadata().getSampleBlock());
    return sinker;
}


QueryProcessingStage::Enum StorageLocalShuffle::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info) const
{
    LOG_TRACE(logger, "query:{}, to_stage:{}, query_kind:{}", queryToString(query_info.query), to_stage, local_context->getClientInfo().query_kind);
    if (local_context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        // When there is join in the query, cannot enable the two phases processing. It will cause
        // a column missing exception, if the result column is in the right table but not in the left table
        auto select_query = query_info.query->as<ASTSelectQuery &>();
        if (select_query.join())
            return QueryProcessingStage::FetchColumns;

        if (to_stage >= QueryProcessingStage::WithMergeableState)
            return QueryProcessingStage::WithMergeableState;
    }

    return QueryProcessingStage::FetchColumns;
}

StorageShuffleClose::StorageShuffleClose(
    ContextPtr context_,
    ASTPtr query_,
    const ColumnsDescription & columns_,
    const String & cluster_name_,
    const String & session_id_,
    const String & table_id_)
    : IStorage(StorageID(session_id_, table_id_ + "_closed"))
    , WithContext(context_)
    , query(query_)
    , cluster_name(cluster_name_)
    , session_id(session_id_)
    , table_id(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageShuffleClose::read(
    const Names & /*column_names_*/,
    const StorageSnapshotPtr & /*metadata_snapshot_*/,
    SelectQueryInfo & /*query_info_*/,
    ContextPtr /*context_*/,
    QueryProcessingStage::Enum /*processed_stage_*/,
    size_t /*max_block_size_*/,
    unsigned /*num_streams_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageShuffleClose has no read implementation");
}

class StorageShuffleCloseSink : public SinkToStorage
{
public:
    explicit StorageShuffleCloseSink(
        ContextPtr context_, const String & cluster_name_, const String & session_id_, const String & table_id_, const Block & header_)
        : SinkToStorage(header_)
        , context(context_)
        , cluster_name(cluster_name_)
        , session_id(session_id_)
        , table_id(table_id_)
        , header(header_)
    {
    }

    String getName() const override { return "StorageShuffleCloseSink"; }

protected:
    void consume(Chunk /*chunk*/) override
    {
        auto session = ShuffleBlockTableManager::getInstance().getSession(session_id);
        if (!session)
            return;
        LOG_TRACE(logger, "session({}), ref={}, tables={}", session_id, session->value().getRefCount(), session->value().dumpTables());
        auto table = session->value().getTable(table_id);
        if (!table)
            return;
        table->makeSinkFinished();
        LOG_INFO(logger, "mark table {}.{} sink finished", session_id, table_id);
    }

private:
    ContextPtr context;
    String cluster_name;
    String session_id;
    String table_id;
    Block header;
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleCloseSink");
};

SinkToStoragePtr StorageShuffleClose::write(const ASTPtr & /*ast*/, const StorageMetadataPtr & /*storage_metadata*/, ContextPtr context_)
{
    auto sinker
        = std::make_shared<StorageShuffleCloseSink>(context_, cluster_name, session_id, table_id, getInMemoryMetadata().getSampleBlock());
    return sinker;
}

}
