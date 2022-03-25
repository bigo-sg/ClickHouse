#include <memory>
#include <Interpreters/InterpreterTreeQuery.h>
#include <Common/ErrorCodes.h>
#include "Core/Block.h"
#include "Core/QueryProcessingStage.h"
#include "Interpreters/InterpreterInsertQuery.h"
#include "Interpreters/InterpreterSelectWithUnionQuery.h"
#include "QueryPipeline/QueryPipeline.h"
#include "base/logger_useful.h"
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
    LOG_TRACE(logger, "header: {} for({})", res->pipeline.getHeader().dumpNames(), queryToString(query_));
    return res;
}

InterpreterTreeQuery::BlockIOPtr InterpreterTreeQuery::buildInsertBlockIO(std::shared_ptr<ASTInsertQuery> insert_query)
{
    auto distributed_queries = tryToMakeDistributedInsertQueries(insert_query);
    if (!distributed_queries)
    {
        auto interpreter = InterpreterInsertQuery(insert_query, context);
        #if 0
        auto insert_block_io = interpreter.execute();
        QueryPipelineBuilder pipeline_builder;
        pipeline_builder.init(Pipe(Processors{std::make_shared<BlockIOSourceTransform>(insert_block_io)}));
        auto res = std::make_shared<BlockIO>();
        res->pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
        #else
        auto res = std::make_shared<BlockIO>(interpreter.execute());
        #endif
        return res;
    }

    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    Pipes pipes;
    for (auto & shard_query : *distributed_queries)
    {
        auto & node = shard_query.first;
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

std::optional<std::list<std::pair<Cluster::Address, String>>> InterpreterTreeQuery::tryToMakeDistributedInsertQueries(ASTPtr from_query)
{
    LOG_TRACE(logger, "tryToMakeDistributedInsertQueries. query:{}", queryToString(from_query));
    #if 1
    // just for test
    std::list<std::pair<Cluster::Address, String>> res;
    String query_str = queryToString(from_query);
    auto cluster = context->getCluster(context->getSettings().distributed_shuffle_join_cluster.value)->getClusterWithReplicasAsShards(context->getSettingsRef());
    for (const auto & shard : cluster->getShardsAddresses())
    {
        for (const auto & node : shard)
        {
            res.push_back(std::make_pair(node, query_str));
        }
    }
    return res;
    #else
    return {};
    #endif
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
        auto & node = shard_query.first;
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

std::optional<std::list<std::pair<Cluster::Address, String>>> InterpreterTreeQuery::tryToMakeDistributedSelectQueries(ASTPtr from_query)
{
    LOG_TRACE(logger, "tryToMakeDistributedSelectQueries. query:{}", queryToString(from_query));
    #if 0
    // just for test
    std::list<std::pair<Cluster::Address, String>> res;
    String query_str = queryToString(from_query);
    auto cluster = context->getCluster(context->getSettings().distributed_shuffle_join_cluster.value)->getClusterWithReplicasAsShards(context->getSettingsRef());
    for (const auto & shard : cluster->getShardsAddresses())
    {
        for (const auto & node : shard)
        {
            res.push_back(std::make_pair(node, query_str));
        }
    }
    return res;
    #else
    return {};
    #endif
}

InterpreterTreeQuery::BlockIOPtr InterpreterTreeQuery::buildTreeBlockIO(std::shared_ptr<ASTTreeQuery> tree_query)
{
    InterpreterTreeQuery interpreter(tree_query, context, options);
    return std::make_shared<BlockIO>(interpreter.execute());
}

}
