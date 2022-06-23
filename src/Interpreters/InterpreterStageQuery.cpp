#include <algorithm>
#include <memory>
#include <mutex>
#include <utility>
#include <Core/Block.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterStageQuery.h>
#include <Interpreters/StorageDistributedTasksBuilder.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTStageQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/StageQueryStep.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/StageQueryTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/DistributedShuffle/StorageShuffle.h>
#include <TableFunctions/TableFunctionShuffle.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <boost/concept_archetype.hpp>
#include <Common/ErrorCodes.h>
#include <Common/Stopwatch.h>
#include <Interpreters/ASTRewriters/CollectQueryStoragesAction.h>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/ASTRewriters/CollectQueryStoragesAction.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}
InterpreterStageQuery::InterpreterStageQuery(ASTPtr query_, ContextPtr context_, SelectQueryOptions options_)
    : query(query_)
    , context(context_)
    , options(options_)
{
}

BlockIO InterpreterStageQuery::execute()
{
    Stopwatch watch;
    auto stage_query = std::dynamic_pointer_cast<ASTStageQuery>(query);
    if (!stage_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTStageQuery is expected, but we get {}", query->getID());
    QueryBlockIOs input_block_ios;
    std::mutex input_block_ios_mutex;
    auto build_block_io_task = [&](ASTPtr input_query)
    {
        auto res = buildBlockIO(input_query);
        std::lock_guard<std::mutex> lock{input_block_ios_mutex};
        input_block_ios.emplace_back(res);
    };
    const size_t default_thread_nums = 1;
    size_t thread_nums = std::min(stage_query->upstream_queries.size() + 1, std::max(default_thread_nums, stage_query->upstream_queries.size() + 1));
    // Since it may need to query informations from outside storages, and the cost is high, make it run currently
    ThreadPool thread_pool{thread_nums};
    for (auto & input_query : stage_query->upstream_queries)
    {
        thread_pool.scheduleOrThrowOnError([&input_query, build_block_io_task]() { build_block_io_task(input_query); });
        //input_block_ios.emplace_back(buildBlockIO(input_query));
    }
    QueryBlockIO output_block_io;
    thread_pool.scheduleOrThrowOnError(
        [&]()
        {
            output_block_io = buildBlockIO(stage_query->current_query);
        });
    thread_pool.wait();
    //auto output_block_io = buildBlockIO(stage_query->current_query);

    auto res = execute(output_block_io, input_block_ios);
    return res;
}

BlockIO InterpreterStageQuery::execute(const QueryBlockIO & output_io, const QueryBlockIOs & input_block_ios)
{
    QueryPlan query_plan;
    if (couldRunParallelly(query->as<ASTStageQuery>()))
    {
        query_plan.addStep(std::make_unique<ParallelStageQueryStep>(context, output_io, input_block_ios));
    }
    else
    {
        query_plan.addStep(std::make_unique<StageQueryStep>(context, output_io, input_block_ios));
    }
    auto pipeline_builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context));
    BlockIO res;
    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    return res;
}

QueryBlockIO InterpreterStageQuery::buildBlockIO(ASTPtr query_)
{
    QueryBlockIO res;
    if (query_->as<ASTInsertQuery>())
    {
        res = buildInsertBlockIO(query_);
    }
    else if (query_->as<ASTSelectWithUnionQuery>())
    {
        res = buildSelectBlockIO(query_);
    }
    else if (query_->as<ASTStageQuery>())
    {
        res = buildTreeBlockIO(query_);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknow query type : {}", query_->getID());
    }
    return res;
}

QueryBlockIO InterpreterStageQuery::buildInsertBlockIO(ASTPtr insert_query)
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
                "InterpreterStageQuery remote insert",
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

std::vector<StoragePtr> InterpreterStageQuery::getSelectStorages(ASTPtr ast)
{
    CollectQueryStoragesAction action(context);
    ASTDepthFirstVisitor<CollectQueryStoragesAction> visitor(action, ast);
    return visitor.visit();
}

ASTPtr InterpreterStageQuery::unwrapSingleSelectQuery(const ASTPtr & ast)
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

std::optional<std::list<std::pair<DistributedTask, String>>> InterpreterStageQuery::tryToMakeDistributedInsertQueries(ASTPtr from_query)
{
    String cluster_name = context->getSettings().use_cluster_for_distributed_shuffle.value;
    auto * insert_query = from_query->as<ASTInsertQuery>();

    if (insert_query->table_function && insert_query->table_function->as<ASTFunction>()->name == TableFunctionClosedShuffle::name)
    {
        String query_str = queryToString(from_query);
        query_str += " (1)";

        std::list<std::pair<DistributedTask, String>> res;
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
    auto * union_select_query = insert_query->select->as<ASTSelectWithUnionQuery>();
    for (auto & child : union_select_query->list_of_selects->children)
    {
        auto * select_query = child->as<ASTSelectQuery>();
        if (select_query->limitBy() || select_query->limitByLength() || select_query->limitLength() || select_query->limitOffset()
            || select_query->limitByOffset())
            return {};
    }
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
        if (storages[0]->getName() == StorageLocalShuffle::NAME)
            return {};
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
    auto query_str = queryToString(from_query);
    for (const auto & task : tasks)
    {
        res.emplace_back(std::make_pair(task, query_str));
    }
    return res;
}

QueryBlockIO InterpreterStageQuery::buildSelectBlockIO(ASTPtr select_query)
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
                "InterpreterStageQuery remote select",
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

std::list<std::pair<DistributedTask, String>> InterpreterStageQuery::buildSelectTasks(ASTPtr from_query)
{
    std::list<std::pair<DistributedTask, String>> res;
    String query_str = queryToString(from_query);
    String cluster_name = context->getSettings().use_cluster_for_distributed_shuffle.value;
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
std::optional<std::list<std::pair<DistributedTask, String>>> InterpreterStageQuery::tryToMakeDistributedSelectQueries(ASTPtr from_query)
{
    auto storages = getSelectStorages(from_query);
    bool has_groupby = ASTAnalyzeUtil::hasGroupByRecursively(from_query);
    bool has_agg = ASTAnalyzeUtil::hasAggregationColumnRecursively(from_query);

    // if the query has order by or limit, run in single node
    auto * union_select_query = from_query->as<ASTSelectWithUnionQuery>();
    for (auto & child : union_select_query->list_of_selects->children)
    {
        auto * select_query = child->as<ASTSelectQuery>();
        if (select_query->orderBy() || select_query->limitBy() || select_query->limitByLength() || select_query->limitLength()
            || select_query->limitOffset() || select_query->limitByOffset())
        {
            LOG_TRACE(logger, "query has order by or limit. [{}] {}", child->getID(), queryToString(child));
            return {};
        }
    }

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
    else if (storages.size() == 1)
    {
        if (storages[0]->getName() == StorageShuffleAggregation::NAME)
        {
            if (!has_groupby)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "There should be group by clause here. query: {}", queryToString(from_query));
            return buildSelectTasks(from_query);
        }
    }
    return {};

}

QueryBlockIO InterpreterStageQuery::buildTreeBlockIO(ASTPtr stage_query)
{
    InterpreterStageQuery interpreter(stage_query, context, options);
    return {.block_io = std::make_shared<BlockIO>(interpreter.execute()), .query = stage_query};
}

bool InterpreterStageQuery::couldRunParallelly(const ASTStageQuery * query)
{
    if (query->upstream_queries.size() != 1)
        return true;

    const auto * insert_query = query->upstream_queries[0]->as<ASTInsertQuery>();
    if (!insert_query)
        return true;

    const auto * event_insert_query = query->current_query->as<ASTInsertQuery>();
    if (!event_insert_query)
        return true;

    if (event_insert_query->table_function->as<ASTFunction>()->name != TableFunctionClosedShuffle::name)
        return true;
    return false;
}

}
