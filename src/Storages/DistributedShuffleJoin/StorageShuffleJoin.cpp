#include <algorithm>
#include <memory>
#include <mutex>
#include <utility>
#include <IO/Operators.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/Cluster.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Chunk.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPipeline/RemoteInserter.h>
#include <Storages/DistributedShuffleJoin/HashedBlocksStorage.h>
#include <Storages/DistributedShuffleJoin/StorageShuffleJoin.h>
#include <base/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/FilterDescription.h>
#include <Common/Exception.h>
#include "Interpreters/InterpreterSelectQuery.h"
#include "base/types.h"
#include <Core/QueryProcessingStage.h>
#include <base/defines.h>
#include <Interpreters/Cluster.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Parsers/ASTSelectQuery.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Poco/Logger.h>

#include <Common/Stopwatch.h>

#include <sys/time.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
class StorageShuffleJoinSource : public SourceWithProgress, WithContext
{
public:
    StorageShuffleJoinSource(ContextPtr context_, const String & session_id_, const String & table_id_, const Block & header_)
        : SourceWithProgress(header_)
        , WithContext(context_)
        , session_id(session_id_)
        , table_id(table_id_)
        , header(header_)
    {
    }

    ~StorageShuffleJoinSource() override
    {
        if (table_storage)
        {
            session_storage->releaseTable(table_id);
        }
    }

    String getName() const override { return "StorageShuffleJoinSource"; }
    Chunk generate() override
    {
        tryInitialize();
        if (unlikely(!table_storage))
        {
            LOG_INFO(logger, "{}.{} is not found.", session_id, table_id);
            return {};
        }
        #if 0
        if (unlikely(!table_storage->getChunkSizeWithoutMutex()))
        {
            LOG_INFO(
                logger,
                "Finished reading table({}.{}). chunks: {}, rows: {}",
                table_storage->getSessionId(),
                table_storage->getTableId(),
                table_storage->getChunksNum(),
                read_rows);
            return {};
        }
        Chunk res = table_storage->popChunkWithoutMutex();
        //LOG_TRACE9logger, "{}.{} read rows:{}. ", table_storage->getSessionId(), table_storage->getTableId(), res.getNumRows());
        read_rows += res.getNumRows();
        if (!res.getNumRows())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The chunk should not be empty. table {}.{}", session_id, table_id);
        //LOG_TRACE9logger, "{}.{} generate rows:{}. read_rows:{}", table_storage->getSessionId(), table_storage->getTableId(), res.getNumRows(), read_rows);
        return res;
        #else
        Chunk res = table_storage->popChunk();
        if (!res.hasRows())
        {
            LOG_TRACE(logger, "reading finish. table:{}.{}", session_id, table_id);
        }
        if (!read_rows)
        {
            LOG_TRACE(logger, "read first chunk. {}.{}", session_id, table_id);
        }
        read_rows += res.getNumRows();
        //LOG_TRACE9logger, "{}.{} generate rows:{}. read_rows:{}", table_storage->getSessionId(), table_storage->getTableId(), res.getNumRows(), read_rows);
        return res;
        #endif
    }
private:
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleJoinSource");
    bool has_initialized = false;
    String session_id;
    String table_id;
    Block header;
    SessionHashedBlocksTablesStoragePtr session_storage;
    TableHashedBlocksStoragePtr table_storage;
    size_t read_rows = 0;

    void tryInitialize()
    {
        if (likely(has_initialized))
            return;
        //LOG_TRACE9logger, "initialize table source. {}.{}", session_id, table_id);
        session_storage = HashedBlocksStorage::getInstance().getOrSetSession(session_id);
        if (session_storage)
        {
            table_storage = session_storage->getTable(table_id, true);
            if (table_storage)
            {
                #if 0
                table_storage->getMutex().lock();
                #endif
            }
            else
            {
                //LOG_TRACE9logger, "Not found table:{}-{}", session_id, table_id);
            }
        }
        else {
            //LOG_TRACE9logger, "Not found session:{}", session_id);
        }
        
        has_initialized = true;
    }
};

class StorageShuffleJoinSink : public SinkToStorage
{
public:
    explicit StorageShuffleJoinSink(
        ContextPtr context_,
        const String & cluster_,
        const String & session_id_,
        const String & table_id_,
        const Block & header_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_,
        UInt64 active_sinks_)
        : SinkToStorage(header_), context(context_), cluster(cluster_), session_id(session_id_), table_id(table_id_)
        , columns_desc(columns_)
        , hash_expr_list(hash_expr_list_)
        , active_sinks(active_sinks_)
    {
    }

    void onFinish() override
    {
        LOG_TRACE(logger, "{}.{} finish sinking", session_id, table_id);
        // Just for signal remote nodes that this node has finished loading
        if (unlikely(!has_initialized))
        {
            initOnce();
        }
        for (auto & inserter : node_inserters)
        {
            Block empty_block = getInputPort().getHeader();
            //LOG_TRACE9logger, "send an empty block:{} {}", empty_block.dumpNames(), empty_block.rows());
            inserter->write(empty_block);
            inserter->onFinish();
        }
        if (watch)
        {
            watch->stop();
            size_t elapse = watch->elapsedMilliseconds();
            LOG_TRACE(logger, "{}.{} sink elapsed. {}", session_id, table_id, elapse);
        }
    }
    String getName() const override { return "StorageShuffleJoinSink"; }
protected:
    // split the block into multi blocks, and send to different nodes
    void consume(Chunk chunk) override
    {
        if (unlikely(!has_initialized))
        {
            initOnce();
        }
        std::vector<Block> splited_blocks;
        auto chunk_columns = chunk.detachColumns();
        Block original_block = getInputPort().getHeader().cloneWithColumns(chunk_columns);
        //LOG_TRACE9logger, "header:{}, block names:{}", getInputPort().getHeader().dumpNames(), original_block.dumpNames());
        splitBlock(original_block, &splited_blocks);
        sendBlocks(splited_blocks);
    }
private:
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleJoinSink");
    ContextPtr context;
    String cluster;
    String session_id;
    String table_id;
    ColumnsDescription columns_desc;
    ASTPtr hash_expr_list;
    UInt64 active_sinks;
    Strings hash_expr_columns_names;

    std::mutex init_mutex;
    std::atomic<bool> has_initialized = false;
    std::vector<std::shared_ptr<RemoteInserter>> node_inserters;
    std::vector<std::shared_ptr<Connection>> node_connections;
    std::shared_ptr<ExpressionActions> hash_expr_cols_actions;

    std::unique_ptr<Stopwatch> watch;

    void initOnce()
    {
        watch = std::make_unique<Stopwatch>();
        std::lock_guard lock(init_mutex);
        if (has_initialized)
            return;
        initInserters();
        initHashExpressionActions();
        has_initialized = true;
        LOG_TRACE(logger, "{}.{} consume first chunk", session_id, table_id);
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
        insert_sql = fmt::format("INSERT INTO FUNCTION distHashedChunksStorage('{}', '{}', '{}', {}) VALUES", session_id, table_id, write_buf.str(), active_sinks);
        //LOG_TRACE9logger, "insert sql: {}", insert_sql);


        // prepare remote call
        auto cluster_addresses = getSortedShardAddresses();
        node_inserters.reserve(cluster_addresses.size());
        for (const auto & node : cluster_addresses)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "StorageShuffleJoinSink",
                node.compression,
                node.secure);
            node_connections.emplace_back(connection);
            auto inserter = std::make_shared<RemoteInserter>(
                *connection,
                ConnectionTimeouts{3000000, 3000000, 3000000},
                insert_sql,
                context->getSettings(),
                context->getClientInfo());
            node_inserters.emplace_back(inserter);
        }

    }

    void initHashExpressionActions()
    {
        // prepare split expr
        auto names_and_types = columns_desc.getAllPhysical();
        String hash_expr_cols_str;
        auto hash_expr_list_str = queryToString(hash_expr_list);
        WriteBufferFromOwnString write_buf;
        size_t n = node_inserters.size();
        for (size_t i = 0; i < n; ++i)
        {
            if (i)
                write_buf << ",";
            write_buf << "cityHash64(" << hash_expr_list_str << ")%" << n << "=" << i;
            //write_buf << "cityHash64(" << "a" << ")%" << n << "=" << i;
        }
        hash_expr_cols_str = write_buf.str();
        //LOG_TRACE9logger, "hash_expr_cols_str: {}", hash_expr_cols_str);

        auto settings = context->getSettings();
        ParserExpressionList hash_expr_list_parser(true);
        ASTPtr fun_ast = parseQuery(
            hash_expr_list_parser, hash_expr_cols_str, "parsing hash expression list", settings.max_query_size, settings.max_parser_depth);
        for (auto & child : fun_ast->children)
        {
            hash_expr_columns_names.emplace_back(child->getColumnName());
        }
        DebugASTLog<true> visit_log;
        ActionsDAGPtr actions = std::make_shared<ActionsDAG>(names_and_types);
        PreparedSets prepared_sets;
        SubqueriesForSets subqueries_for_sets;
        ActionsVisitor::Data visitor_data(
            context,
            SizeLimits{settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode},
            10,
            names_and_types,
            std::move(actions),
            prepared_sets,
            //write_buf << "cityHash64(" << hash_expr_list_str << ")%" << n << "=" << i;
            subqueries_for_sets,
            true,
            false,
            true,
            false);
        ActionsVisitor(visitor_data, visit_log.stream()).visit(fun_ast);
        actions = visitor_data.getActions();
        hash_expr_cols_actions = std::make_shared<ExpressionActions>(actions);
        //LOG_TRACE9logger, "hash_expr_cols_actions: {}", hash_expr_cols_actions->dumpActions());
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

    void splitBlock(Block & original_block, std::vector<Block> * splited_blocks)
    {
        size_t num_rows_before_filtration = original_block.rows();
        //LOG_TRACE9logger, "before trans, columns size:{}", original_block.columns());
        //LOG_TRACE9logger, "expression actions:{}", hash_expr_cols_actions->dumpActions());
        hash_expr_cols_actions->execute(original_block, num_rows_before_filtration);
        //LOG_TRACE9logger, "after trans, columns size:{}", original_block.columns());

        for (const auto & column_name : hash_expr_columns_names)
        {
            
            auto * column = original_block.findByName(column_name);
            if (unlikely(!column))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found column ({}) in block({})", column_name, original_block.dumpNames());
            }
            size_t rows = column->column->size();
            for (size_t i = 0 ; i < rows; ++i)
            {
                //LOG_TRACE9logger, "column_name:{} {} - {}", i , column_name, column->column->get64(i));
            }
        }

        auto header = getInputPort().getHeader();
        for (const auto & filter_column_name : hash_expr_columns_names)
        {
            auto full_column = original_block.findByName(filter_column_name)->column->convertToFullColumnIfConst();
            auto filter_desc = std::make_unique<FilterDescription>(*full_column);
            auto num_filtered_rows = filter_desc->countBytesInFilter();
            //LOG_TRACE9logger, "num_filtered_rows:{} @ {} for {}", num_filtered_rows, filter_column_name, table_id);
            ColumnsWithTypeAndName new_columns;
            for (size_t i = 0; i < header.columns(); ++i)
            {
                auto & from_column = original_block.getByPosition(i);
                //LOG_TRACE9logger, "@{} colums size:{}, filter size:{} for {}", i, from_column.column->size(), filter_desc->data->size(), table_id);
                auto new_column = filter_desc->filter(*from_column.column, num_filtered_rows);
                new_columns.emplace_back(new_column, from_column.type, from_column.name);

            }
            splited_blocks->emplace_back(new_columns);
            //Block & splited_block = splited_blocks->back();
            //LOG_TRACE9logger, "new block cols: {}, rows:{} for {}", splited_block.columns(), splited_block.rows(), table_id);
            #if 0
            for (size_t i = 0; i < splited_block.columns(); ++i)
            {
                auto column = splited_block.getByPosition(i);
                size_t rows = column.column->size();
                for (size_t k = 0; k < rows; ++k)
                {
                    //LOG_TRACE9logger, "column pos:{}, row: {}, value:{} for {}", i, k, column.column->get64(i), table_id);
                }
            }
            #endif
        } 
    }
    void sendBlocks(std::vector<Block> & blocks)
    {
        for(size_t i = 0; i < blocks.size(); ++i)
        {
            auto & block = blocks[i];
            auto & inserter = node_inserters[i];
            //LOG_TRACE9logger, "send block. rows:{}, cols:{}, names:{}, inserter:{}", block.rows(), block.columns(), block.dumpNames(), i);
            if (block.rows())
                inserter->write(block);
        }
    }
};

/**
 * FIXEDME: Maybe need the initiator pass the cluster nodes, not query by the worker nodes. Since the cluster nodes set may change
 */
StorageShuffleBase::StorageShuffleBase(
    ContextPtr context_,
    ASTPtr query_,
    const String & cluster_name_,
    const String & session_id_,
    const String & table_id_,
    const ColumnsDescription & columns_,
    ASTPtr hash_expr_list_,
    UInt64 active_sinks_)
    : IStorage(StorageID(session_id_, table_id_))
    , WithContext(context_)
    , query(query_)
    , cluster_name(cluster_name_)
    , session_id(session_id_)
    , table_id(table_id_)
    , hash_expr_list(hash_expr_list_)
    , active_sinks(active_sinks_)
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
    unsigned /*num_streams_*/)
{
    auto header = getInMemoryMetadata().getSampleBlock();
    WriteBufferFromOwnString write_buf;
    for (const auto & name : column_names_)
    {
        write_buf << name << ",";
    }
    
    auto query_kind = context_->getClientInfo().query_kind;
    //LOG_TRACE9logger, "header:{}. to read columns:{}. query_kind:{}, stage:{}",
    //    header.dumpNames(), write_buf.str(), query_kind,
    //    processed_stage_);
    //LOG_TRACE9logger, "query:{}", queryToString(query_info_.query));
    //LOG_TRACE9logger, "original query:{}", queryToString(query_info_.original_query));
    if (query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
    {
        //LOG_TRACE9logger, "Run local query. query_kind:{}, query:{}", query_kind, queryToString(query_info_.query));
        auto source = std::make_shared<StorageShuffleJoinSource>(context_, session_id, table_id, header);
        return Pipe(source);
    }
#if 0
    if (processed_stage_ >= QueryProcessingStage::Enum::WithMergeableState)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageShuffleJoin doesn't support two phases merge processing. current processing stage:{}", processed_stage_);
    }
#endif
    // Since the query_info_.query has been rewritten, it may cause an ambiguous column exception in join case.
    // So we use the original_query here.
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
                connection,
                remote_query,
                header,
                context_,
                nullptr,
                scalars,
                Tables(),
                processed_stage_,
                RemoteQueryExecutor::Extension{});
            //LOG_TRACE9logger, "run query on node:{}. query:{}", node.host_name, remote_query);
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false));
        }
    }
    metadata_snapshot_->check(column_names_);
    return Pipe::unitePipes(std::move(pipes));

}

SinkToStoragePtr StorageShuffleBase::write(const ASTPtr & /*ast*/, const StorageMetadataPtr & /*storage_metadata*/, ContextPtr context_)
{
    //LOG_TRACE9logger, "write query: {}", queryToString(ast));
    auto sinker = std::make_shared<StorageShuffleJoinSink>(
        context_, cluster_name, session_id, table_id, getInMemoryMetadata().getSampleBlock(), getInMemoryMetadata().getColumns(), hash_expr_list, active_sinks);
    return sinker;
}

#if 1
QueryProcessingStage::Enum StorageShuffleBase::getQueryProcessingStage(
        ContextPtr local_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info) const
{
    //LOG_TRACE9logger, "query:{}, to_stage:{}, query_kind:{}", queryToString(query_info.query), to_stage, local_context->getClientInfo().query_kind);
    if (local_context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        #if 1
        // When there is join in the query, cannot enable the two phases processing. It will cause 
        // a column missing exception, if the result column is in the right table but not in the left table
        auto select_query = query_info.query->as<ASTSelectQuery &>();
        if (select_query.join())
            return QueryProcessingStage::FetchColumns;
        #endif
        if (to_stage >= QueryProcessingStage::WithMergeableState)
            return QueryProcessingStage::WithMergeableState;
    }

    return QueryProcessingStage::FetchColumns;
}
#else
QueryProcessingStage::Enum StorageShuffleBase::getQueryProcessingStage(
        ContextPtr /*local_context*/,
        QueryProcessingStage::Enum /*to_stage*/,
        const StorageSnapshotPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/) const
{
    return QueryProcessingStage::FetchColumns;
}

#endif

const String StorageShuffleJoin::NAME = "StorageShuffleJoin";
StorageShuffleJoin::StorageShuffleJoin(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_,
        UInt64 active_sinks_)
    : StorageShuffleBase(context_, query_, cluster_name_, session_id_, table_id_, columns_, hash_expr_list_, active_sinks_)
{
    logger = &Poco::Logger::get("StorageShuffleJoin");
}
 

const String StorageShuffleAggregation::NAME = "StorageShuffleAggregation";
StorageShuffleAggregation::StorageShuffleAggregation(
        ContextPtr context_,
        ASTPtr query_,
        const String & cluster_name_,
        const String & session_id_,
        const String & table_id_,
        const ColumnsDescription & columns_,
        ASTPtr hash_expr_list_,
        UInt64 active_sinks_)
    : StorageShuffleBase(context_, query_, cluster_name_, session_id_, table_id_, columns_, hash_expr_list_, active_sinks_)
{
    logger = &Poco::Logger::get("StorageShuffleAggregation");
}
class StorageShuffleJoinPartSink : public SinkToStorage
{
public:
    explicit StorageShuffleJoinPartSink(ContextPtr context_, const String & session_id_, const String & table_id_, const Block & header_, UInt64 active_sinks_)
        : SinkToStorage(header_)
        , context(context_)
        , session_id(session_id_)
        , table_id(table_id_)
        , active_sinks(active_sinks_)
    {
        auto session_storage = HashedBlocksStorage::getInstance().getOrSetSession(session_id_);
        if (!session_storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Get session({}) storage failed.", session_id_);
        table_storage = session_storage->getOrSetTable(table_id_, header_, active_sinks);
        if (!table_storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Get session table({}-{}) failed.", session_id_, table_id_);
    }

    void onFinish() override
    { 
        //LOG_TRACE9logger, "finish sinking.{} table:{}.{}", reinterpret_cast<UInt64>(this), session_id, table_id);
        table_storage->increaseFinishedSinkCount();
    }
    String getName() const override { return "StorageShuffleJoinPartSink"; }
protected:
    void consume(Chunk chunk) override
    {
        //LOG_TRACE9logger, "table {}.{} sink a chunk. rows:{}", session_id, table_id, chunk.getNumRows());
        table_storage->addChunk(std::move(chunk));
    }
private:
    ContextPtr context;
    String session_id;
    String table_id;
    UInt64 active_sinks;
    TableHashedBlocksStoragePtr table_storage;
    Poco::Logger * logger = &Poco::Logger::get("StorageShuffleJoinPartSink");
};


StorageShuffleJoinPart::StorageShuffleJoinPart(
    ContextPtr context_, ASTPtr query_, const String & session_id_, const String & table_id_, const ColumnsDescription & columns_, UInt64 active_sinks_)
    : IStorage(StorageID(session_id_, table_id_ + "_part"))
    , WithContext(context_)
    , query(query_)
    , session_id(session_id_)
    , table_id(table_id_)
    , active_sinks(active_sinks_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageShuffleJoinPart::read(
    const Names & /*column_names_*/,
    const StorageSnapshotPtr & /*metadata_snapshot_*/,
    SelectQueryInfo & /*query_info_*/,
    ContextPtr /*context_*/,
    QueryProcessingStage::Enum /*processed_stage_*/,
    size_t /*max_block_size_*/,
    unsigned /*num_streams_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "read() is no implemented for StorageShuffleJoinPart");
}

SinkToStoragePtr StorageShuffleJoinPart::write(const ASTPtr & /*ast*/, const StorageMetadataPtr & /*storage_metadata*/, ContextPtr context_)
{
    //LOG_TRACE9logger, "write query: {}", queryToString(ast));
    auto sinker = std::make_shared<StorageShuffleJoinPartSink>(context_, session_id, table_id, getInMemoryMetadata().getSampleBlock(), active_sinks);
    return sinker;
}
}
