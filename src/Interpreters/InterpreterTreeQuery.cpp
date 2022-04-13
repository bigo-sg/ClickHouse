#include <algorithm>
#include <memory>
#include <mutex>
#include <Core/Block.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/CollectStoragesVisitor.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterTreeQuery.h>
#include <Interpreters/StorageDistributedTasksBuilder.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTreeQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/TreeQueryStep.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/TreeQueryTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/DistributedShuffleJoin/StorageShuffleJoin.h>
#include <TableFunctions/TableFunctionShuffleJoin.h>
#include <base/logger_useful.h>
#include <base/types.h>
#include "Common/Stopwatch.h"
#include <Common/ErrorCodes.h>


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
    Stopwatch watch;
    auto tree_query = std::dynamic_pointer_cast<ASTTreeQuery>(query);
    if (!tree_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTTreeQuery is expected, but we get {}", query->getID());
    QueryBlockIOs input_block_ios;
    std::mutex input_block_ios_mutex;
    auto build_block_io_task = [&](ASTPtr input_query){
        auto res = buildBlockIO(input_query);
        std::lock_guard<std::mutex> lock{input_block_ios_mutex};
        input_block_ios.emplace_back(res);
    };
    const size_t default_thread_nums = 6;
    size_t thread_nums = std::min(tree_query->input_asts.size() + 1, std::max(default_thread_nums, tree_query->input_asts.size() + 1));
    // Since it may need to query informations from outside storages, and the cost is high, make it run currently
    ThreadPool thread_pool{thread_nums};
    for (auto & input_query : tree_query->input_asts)
    {
        thread_pool.scheduleOrThrowOnError([&input_query, build_block_io_task]() { build_block_io_task(input_query); });
        //input_block_ios.emplace_back(buildBlockIO(input_query));
    }
    QueryBlockIO output_block_io;
    thread_pool.scheduleOrThrowOnError(
        [&]()
        {
            output_block_io = buildBlockIO(tree_query->output_ast);
        });
    thread_pool.wait();
    //auto output_block_io = buildBlockIO(tree_query->output_ast);

    auto res = execute(output_block_io, input_block_ios);
    return res;
}

BlockIO InterpreterTreeQuery::execute(const QueryBlockIO & output_io, const QueryBlockIOs & input_block_ios)
{
    QueryPlan query_plan;
    query_plan.addStep(std::make_unique<ParallelTreeQueryStep>(context, output_io, input_block_ios));
    auto pipeline_builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context));
    pipeline_builder->addInterpreterContext(context);
    BlockIO res;
    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    return res;
}

QueryBlockIO InterpreterTreeQuery::buildBlockIO(ASTPtr query_)
{
    QueryBlockIO res;
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

QueryBlockIO InterpreterTreeQuery::buildInsertBlockIO(std::shared_ptr<ASTInsertQuery> insert_query)
{
    Stopwatch watch;
    auto distributed_queries = tryToMakeDistributedInsertQueries(insert_query);
    if (!distributed_queries)
    {
        auto interpreter = InterpreterInsertQuery(insert_query, context);
        auto res = std::make_shared<BlockIO>(interpreter.execute());
        return {.block_io = res, .query = insert_query};
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
    res->pipeline.setNumThreads(3);
    return {.block_io = res, .query = insert_query};
}

ASTPtr InterpreterTreeQuery::fillHashedChunksStorageSinks(ASTPtr from_query, UInt64 sinks)
{
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(from_query);
    if (!insert_query)
    {
        return from_query;
    }
    if (!insert_query->table_function)
    {
        return from_query;
    }
    auto table_function = std::dynamic_pointer_cast<ASTFunction>(insert_query->table_function);
    if (table_function->name != TableFunctionShuffleJoin::name && table_function->name != TableFunctionShuffleAggregation::name)
    {
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

    return from_query;
    
}

std::vector<StoragePtr> InterpreterTreeQuery::getSelectStorages(ASTPtr ast)
{
    CollectStoragesMatcher::Data data{.context = context};
    CollectStoragesVisitor(data).visit(ast);
    return data.storages;
}

ASTPtr InterpreterTreeQuery::unwrapSingleSelectQuery(const ASTPtr & ast)
{
    if (auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
    {
        if (select_with_union->list_of_selects->children.size() > 1)
            return nullptr;
        return unwrapSingleSelectQuery(select_with_union->list_of_selects->children[0]);
    }
    else if (auto * select = ast->as<ASTSelectQuery>())
    {
        if (select->join())
            return nullptr;
        auto table_expression = extractTableExpression(*select, 0);
        if (auto * select_ast = table_expression->as<ASTSelectWithUnionQuery>())
        {
            return unwrapSingleSelectQuery(table_expression);
        }
        return ast;
    }
    return nullptr;
}

std::optional<std::list<std::pair<DistributedTask, String>>> InterpreterTreeQuery::tryToMakeDistributedInsertQueries(ASTPtr from_query)
{
    String cluster_name = context->getSettings().distributed_shuffle_cluster.value;
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(from_query);
    auto storages = getSelectStorages(insert_query->select);
    bool has_groupby = ASTAnalyzeUtil::hasGroupByRecursively(from_query);
    bool has_agg = ASTAnalyzeUtil::hasAggregationColumnRecursively(from_query);
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
        //if (storages[0]->getName() != "xxx")
        //    return {};
        auto distributed_tasks_builder = StorageDistributedTaskBuilderFactory::getInstance().getBuilder(storages[0]->getName());
        if (!distributed_tasks_builder)
        {
            LOG_INFO(logger, "Not found builder for {}", storages[0]->getName());
            return {};
        }

        auto nested_select_query = unwrapSingleSelectQuery(insert_query->select);
        if (!nested_select_query)
            return {};
        tasks = distributed_tasks_builder->getDistributedTasks(
            cluster_name, context, nested_select_query, storages[0]);
        if (tasks.empty())
        {
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

QueryBlockIO InterpreterTreeQuery::buildSelectBlockIO(std::shared_ptr<ASTSelectWithUnionQuery> select_query)
{
    auto distributed_queries = tryToMakeDistributedSelectQueries(select_query);
    if (!distributed_queries)
    {   
        InterpreterSelectWithUnionQuery interpreter(select_query, context, options);
        auto res = std::make_shared<BlockIO>(interpreter.execute());
        return {.block_io = res, .query = select_query};
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
    return {.block_io = res, .query = select_query};
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
        }
    }
    return res;

}
std::optional<std::list<std::pair<DistributedTask, String>>> InterpreterTreeQuery::tryToMakeDistributedSelectQueries(ASTPtr from_query)
{
    auto storages = getSelectStorages(from_query);
    bool has_groupby = ASTAnalyzeUtil::hasGroupByRecursively(from_query);
    bool has_agg = ASTAnalyzeUtil::hasAggregationColumnRecursively(from_query);
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

QueryBlockIO InterpreterTreeQuery::buildTreeBlockIO(std::shared_ptr<ASTTreeQuery> tree_query)
{
    InterpreterTreeQuery interpreter(tree_query, context, options);
    return {.block_io = std::make_shared<BlockIO>(interpreter.execute()), .query = tree_query};
}

}
