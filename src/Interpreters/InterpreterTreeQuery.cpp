#include <memory>
#include <Interpreters/InterpreterTreeQuery.h>
#include <Common/ErrorCodes.h>
#include "Core/Block.h"
#include "Core/QueryProcessingStage.h"
#include "Interpreters/InterpreterInsertQuery.h"
#include "Interpreters/InterpreterSelectWithUnionQuery.h"
#include "Interpreters/StorageDistributedTasksBuilder.h"
#include "Parsers/ASTInsertQuery.h"
#include "Parsers/IAST_fwd.h"
#include "QueryPipeline/QueryPipeline.h"
#include "base/logger_useful.h"
#include "base/types.h"
#include <Parsers/ASTTreeQuery.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/TreeQueryStep.h>
#include <Processors/Transforms/TreeQueryTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <TableFunctions/TableFunctionShuffleJoin.h>
#include <Interpreters/CollectStoragesVisitor.h>
#include <Interpreters/StorageDistributedTasksBuilder.h>
#include <Storages/DistributedShuffleJoin/StorageShuffleJoin.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
InterpreterTreeQuery::InterpreterTreeQuery(ASTPtr query_, ContextPtr context_, SelectQueryOptions options_)
    : query(query_)
    , context(context_)
    , options(options_)
{
}

BlockIO InterpreterTreeQuery::execute()
{
    auto tree_query = std::dynamic_pointer_cast<ASTTreeQuery>(query);
    if (!tree_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTTreeQuery is expected, but we get {}", query->getID());
    BlockIOs input_block_ios;
    for (auto & input_query : tree_query->input_asts)
    {
        input_block_ios.emplace_back(buildBlockIO(input_query));
    }

    auto output_block_io = buildBlockIO(tree_query->output_ast);

    return execute(output_block_io, input_block_ios);
}

BlockIO InterpreterTreeQuery::execute(BlockIOPtr output_io, BlockIOs & input_block_ios)
{
    QueryPlan query_plan;
    query_plan.addStep(std::make_unique<TreeQueryStep>(output_io, input_block_ios));
    auto pipeline_builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context));
    pipeline_builder->addInterpreterContext(context);
    BlockIO res;
    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    return res;
}

InterpreterTreeQuery::BlockIOPtr InterpreterTreeQuery::buildBlockIO(ASTPtr query_)
{
    BlockIOPtr res;
    if (auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query_))
    {
        res = buildInsertBlockIO(insert_query);
    }
    else if (auto select_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(query_))
    {
        res = buildSelectBlockIO(select_query);
    }
    else if (auto tree_query = std::dynamic_pointer_cast<ASTTreeQuery>(query_))
    {
        res = buildTreeBlockIO(tree_query);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknow query type : {}", query_->getID());
    }
    return res;
}

InterpreterTreeQuery::BlockIOPtr InterpreterTreeQuery::buildInsertBlockIO(std::shared_ptr<ASTInsertQuery> insert_query)
{
    auto distributed_queries = tryToMakeDistributedInsertQueries(insert_query);
    if (!distributed_queries)
    {
        auto interpreter = InterpreterInsertQuery(insert_query, context);
        auto res = std::make_shared<BlockIO>(interpreter.execute());
        return res;
    }

    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    Pipes pipes;
    for (auto & shard_query : *distributed_queries)
    {
        auto & node = shard_query.first.first;
        auto & task_extension = shard_query.first.second;
        auto & remote_query = shard_query.second;
        auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "InterpreterTreeQuery remote insert",
                node.compression,
                node.secure);
        LOG_TRACE(logger, "run on node:{}, query:{}", node.host_name, remote_query);
        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            connection,
            remote_query,
            Block{},
            context,
            nullptr,
            scalars,
            Tables(),
            QueryProcessingStage::Complete,
            task_extension);
        pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(std::move(pipe));
    auto res = std::make_shared<BlockIO>();
    res->pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    return res;
}

ASTPtr InterpreterTreeQuery::fillHashedChunksStorageSinks(ASTPtr from_query, UInt64 sinks)
{
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(from_query);
    if (!insert_query)
    {
        LOG_TRACE(logger, "not insert query: {}", from_query->getID());
        return from_query;
    }
    if (!insert_query->table_function)
    {
        LOG_TRACE(logger, "table_function is null. query:{}", queryToString(from_query));
        return from_query;
    }
    auto table_function = std::dynamic_pointer_cast<ASTFunction>(insert_query->table_function);
    if (table_function->name != TableFunctionShuffleJoin::name)
    {
        LOG_TRACE(logger, "Not {}, query: {}", TableFunctionShuffleJoin::name, queryToString(from_query));
        return from_query;
    }
    auto arg_list = std::dynamic_pointer_cast<ASTExpressionList>(table_function->arguments);
    if (arg_list->children.size() != 5)
    {
        LOG_INFO(logger, "table function's arguments should be 5");
        return from_query;
    }
    auto sinks_arg = std::make_shared<ASTLiteral>(Field(sinks));
    arg_list->children.push_back(sinks_arg);

    LOG_TRACE(logger, "New query: {}", queryToString(from_query));
    return from_query;
    
}

std::vector<StoragePtr> InterpreterTreeQuery::getSelectStorages(ASTPtr ast)
{
    CollectStoragesMatcher::Data data{.context = context};
    CollectStoragesVisitor(data).visit(ast);
    LOG_TRACE(logger, "get storages size: {}", data.storages.size());
    for (const auto & storage : data.storages)
    {
        LOG_TRACE(logger, "get storage:{}, {}", storage->getName(), storage->getStorageID().getNameForLogs());
    }
    return data.storages;
}

bool InterpreterTreeQuery::hasGroupby(const IAST & ast)
{
    if (const auto * insert_query = ast.as<ASTInsertQuery>())
    {
        return hasGroupby(*(insert_query->select));
    }
    else if (const auto * select_with_union_query = ast.as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union_query->list_of_selects->children)
        {
            if (hasGroupby(*child))
                return true;
        }
    }
    else if (const auto * select_query = ast.as<ASTSelectQuery>())
    {
        #if 0
        if (select_query->groupBy() || select_query->having())
            return true;
        const auto * select_list = select_query->select()->as<ASTExpressionList>();
        for (const auto & child : select_list->children)
        {
            if (const auto * function = child->as<ASTFunction>())
            {
                if (function->name == "count" || function->name == "avg" || function->name == "sum")
                {
                    return true;
                }
            }
        }
        #endif
        return select_query->groupBy() != nullptr;

    }
    return false;
}

bool InterpreterTreeQuery::hasAggregation(const IAST & ast)
{
    if (const auto * insert_query = ast.as<ASTInsertQuery>())
    {
        return hasAggregation(*(insert_query->select));
    }
    else if (const auto * select_with_union_query = ast.as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union_query->list_of_selects->children)
        {
            if (hasAggregation(*child))
                return true;
        }
    }
    else if (const auto * select_query = ast.as<ASTSelectQuery>())
    {
        const auto * select_list = select_query->select()->as<ASTExpressionList>();
        for (const auto & child : select_list->children)
        {
            LOG_TRACE(logger, "visit select column:{}", queryToString(child));
            if (const auto * function = child->as<ASTFunction>())
            {
                if (function->name == "count" || function->name == "avg" || function->name == "sum")
                {
                    return true;
                }
            }
        }

    }
    return false;
}

std::optional<std::list<std::pair<DistributedTask, String>>> InterpreterTreeQuery::tryToMakeDistributedInsertQueries(ASTPtr from_query)
{
    LOG_TRACE(logger, "tryToMakeDistributedInsertQueries. query:{}", queryToString(from_query));
    String cluster_name = context->getSettings().distributed_shuffle_cluster.value;
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(from_query);
    auto storages = getSelectStorages(insert_query->select);
    bool has_groupby = hasGroupby(*from_query);
    bool has_agg = hasAggregation(*from_query);
    DistributedTasks tasks;
    if (storages.size() == 2)
    {
        for (const auto & storage : storages)
        {
            if (storage->getName() != StorageShuffleJoin::NAME)
            {
                return {};
            }
        }
        if (has_groupby || has_agg)
            return {};
        auto cluster = context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettings());
        for (const auto & replicas : cluster->getShardsAddresses())
        {
            for (const auto & node : replicas)
            {
                DistributedTask task(node, RemoteQueryExecutor::Extension{});
                tasks.emplace_back(task);
            }
        }
    }
    else if (storages.size() == 1)
    {
        if (storages[0]->getName() != StorageShuffleAggregation::NAME)
            return {};
        auto distributed_tasks_builder = StorageDistributedTaskBuilderFactory::getInstance().getBuilder(storages[0]->getName());
        if (!distributed_tasks_builder)
        {
            LOG_INFO(logger, "Not found builder for {}", storages[0]->getName());
            return {};
        }
        auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(insert_query->select);
        tasks = distributed_tasks_builder->getDistributedTasks(
            cluster_name, context, select_with_union_query->list_of_selects->children[0], storages[0]);
        if (tasks.empty())
        {
            LOG_TRACE(logger, "Tasks is empty for {}", storages[0]->getName());
            return {};
        }
    }
    else
    {
        return {};
    }
    // just for test
    std::list<std::pair<DistributedTask, String>> res;
    auto rewrite_query = fillHashedChunksStorageSinks(from_query, tasks.size());
    String query_str = queryToString(rewrite_query);
    for (const auto & task : tasks)
    {
        res.emplace_back(std::make_pair(task, query_str));
    }
    return res;
}

InterpreterTreeQuery::BlockIOPtr InterpreterTreeQuery::buildSelectBlockIO(std::shared_ptr<ASTSelectWithUnionQuery> select_query)
{
    auto distributed_queries = tryToMakeDistributedSelectQueries(select_query);
    if (!distributed_queries)
    {   
        LOG_TRACE(logger, "initiator run, query:{}", queryToString(select_query));
        InterpreterSelectWithUnionQuery interpreter(select_query, context, options);
        auto res = std::make_shared<BlockIO>(interpreter.execute());
        return res;
    }
    InterpreterSelectWithUnionQuery select_interpreter(select_query, context, options);
    Block header = select_interpreter.getSampleBlock();
    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    Pipes pipes;
    for (auto & shard_query : *distributed_queries)
    {
        auto & node = shard_query.first.first;
        auto task_extension = shard_query.first.second;
        auto & remote_query = shard_query.second;
        auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "InterpreterTreeQuery remote select",
                node.compression,
                node.secure);
        
        LOG_TRACE(logger, "run on node:{}, header: {}, query:{}", node.host_name, header.dumpNames(), remote_query);
        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            connection,
            remote_query,
            header,
            context,
            nullptr,
            scalars,
            Tables(),
            QueryProcessingStage::Complete,
            RemoteQueryExecutor::Extension{});
        pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(std::move(pipe));
    auto res = std::make_shared<BlockIO>();
    res->pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    return res;
}

std::list<std::pair<DistributedTask, String>> InterpreterTreeQuery::buildSelectTasks(ASTPtr from_query)
{
    std::list<std::pair<DistributedTask, String>> res;
    String query_str = queryToString(from_query);
    String cluster_name = context->getSettings().distributed_shuffle_cluster.value;
    auto cluster = context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettings());
    for (const auto & replicas : cluster->getShardsAddresses())
    {
        for (const auto & node : replicas)
        {
            DistributedTask task(node, RemoteQueryExecutor::Extension{});
            res.emplace_back(std::make_pair(task, query_str));
            LOG_TRACE(logger, "run select on node: {}, query:{}", node.host_name, query_str);
        }
    }
    return res;

}
std::optional<std::list<std::pair<DistributedTask, String>>> InterpreterTreeQuery::tryToMakeDistributedSelectQueries(ASTPtr from_query)
{
    LOG_TRACE(logger, "tryToMakeDistributedSelectQueries. query:{}", queryToString(from_query));
    auto storages = getSelectStorages(from_query);
    bool has_groupby = hasGroupby(*from_query);
    bool has_agg = hasAggregation(*from_query);
    LOG_TRACE(logger, "tryToMakeDistributedSelectQueries. query:{}. has_groupby:{}, has_agg:{}", queryToString(from_query), has_groupby, has_agg);
    if (storages.size() == 2)
    {
        for (const auto & storage : storages)
        {
            if (storage->getName() != StorageShuffleJoin::NAME)
            {
                LOG_TRACE(logger, "Not hash storage:{} {}", storage->getName(), StorageShuffleJoin::NAME);
                return {};
            }
        }
        if (has_groupby || has_agg)
            return {};

        return buildSelectTasks(from_query);
        
    }
    else if (storages.size() == 1 && storages[0]->getName() == StorageShuffleAggregation::NAME && has_groupby)
    {
        return buildSelectTasks(from_query);
    }

    return {};

}

InterpreterTreeQuery::BlockIOPtr InterpreterTreeQuery::buildTreeBlockIO(std::shared_ptr<ASTTreeQuery> tree_query)
{
    InterpreterTreeQuery interpreter(tree_query, context, options);
    return std::make_shared<BlockIO>(interpreter.execute());
}

}
