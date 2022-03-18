#include <memory>
#include <Interpreters/InterpreterDistributedShuffleJoinSelectQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterShufflePhasesSelectQuery.h>
#include <Parsers/ASTDistributedShuffleJoinSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTShufflePhasesSelectQuery.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BlockIOPhaseStep.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>
#include "Interpreters/Context.h"
#include "Parsers/queryToString.h"
#include "QueryPipeline/QueryPipelineBuilder.h"
#include <Interpreters/Cluster.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
using BlockIOPtr = std::shared_ptr<BlockIO>;
using BlockIOPtrs = std::vector<BlockIOPtr>;

InterpreterShufflePhasesSelectQuery::InterpreterShufflePhasesSelectQuery(ASTPtr query_, ContextPtr context_, const SelectQueryOptions & options_)
    : IInterpreterUnionOrSelectQuery(query_, context_, options_)
{
    initSampleBlock();
}
BlockIO InterpreterShufflePhasesSelectQuery::execute()
{
    BlockIO res;
    QueryPlan query_plan;
    buildQueryPlan(query_plan);
    auto pipeline_builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context));
    pipeline_builder->addInterpreterContext(context);
    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    return res;
}
void InterpreterShufflePhasesSelectQuery::ignoreWithTotals()
{
    auto select_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(getSelectQuery()->final_query);
    if (!select_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTSelectWithUnionQuery, but we get : {}", queryToString(getSelectQuery()->final_query));
    result_header = InterpreterSelectWithUnionQuery(select_query, context, options).getSampleBlock();
}

void InterpreterShufflePhasesSelectQuery::initSampleBlock()
{
    auto select_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(getSelectQuery()->final_query);
    if (!select_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTSelectWithUnionQuery, but we get : {}", queryToString(getSelectQuery()->final_query));
    result_header = InterpreterSelectWithUnionQuery(select_query, context, options).getSampleBlock();
}

std::shared_ptr<ASTShufflePhasesSelectQuery> InterpreterShufflePhasesSelectQuery::getSelectQuery()
{
    auto shuffle_phases_select_query = std::dynamic_pointer_cast<ASTShufflePhasesSelectQuery>(query_ptr);
    if (!shuffle_phases_select_query)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTDistributedShuffleJoinSelectQuery is expected, but we get : {}", queryToString(query_ptr));
    }
    return shuffle_phases_select_query;
}

void InterpreterShufflePhasesSelectQuery::buildQueryPlan(QueryPlan & query_plan)
{
    auto shuffle_phases_select_query = getSelectQuery();
    std::vector<BlockIOPtrs> shuffle_block_ios;
    for (auto & phase : shuffle_phases_select_query->shuffle_phases)
    {
        shuffle_block_ios.emplace_back(BlockIOPtrs{});
        BlockIOPtrs & block_ios = shuffle_block_ios.back();
        for (auto & query : phase)
        {
            auto block_io = buildShufflePhaseBlockIO(query);
            block_ios.emplace_back(block_io);
        }
    }
    BlockIOPtr select_block_io = buildSelectPhaseBlockIO(shuffle_phases_select_query->final_query);
    DataStream input_stream = {.header = select_block_io->pipeline.getHeader()};
    query_plan.addStep(std::make_unique<BlockIOPhaseStep>(input_stream, select_block_io, shuffle_block_ios));
}

BlockIOPtr InterpreterShufflePhasesSelectQuery::buildShufflePhaseBlockIO(ASTPtr query)
{
    // TODO: if the query can be in cluster mode, construct remote query exectuors
    // otherwise run it on local
    if (!std::dynamic_pointer_cast<ASTInsertQuery>(query))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTInsertQuery is expected, but we get: {}", queryToString(query));
#if 0
    // current run it on local
    auto insert_interprester = std::make_unique<InterpreterInsertQuery>(query, context);
    auto block_io = std::make_shared<BlockIO>();

    return block_io;
#else
    auto cluster = context->getCluster(context->getSettings().distributed_shuffle_join_cluster.value)->getClusterWithReplicasAsShards(context->getSettingsRef());
    Pipes pipes;
    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    for (const auto & shard : cluster->getShardsAddresses())
    {
        for (const auto & node : shard)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "InterpreterShufflePhasesSelectQuery RemoteInsert",
                node.compression,
                node.secure);

            String remote_query = queryToString(query);
            LOG_TRACE(&Poco::Logger::get("executeQuery"), "run on node:{}, query:{}", node.host_name, remote_query);
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection,
                remote_query,
                Block{},
                context,
                nullptr,
                scalars,
                Tables(),
                QueryProcessingStage::FetchColumns,
                RemoteQueryExecutor::Extension{});
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false));
        }
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(std::move(pipe));
    auto res = std::make_shared<BlockIO>();
    res->pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    return res;
#endif
}

BlockIOPtr InterpreterShufflePhasesSelectQuery::buildSelectPhaseBlockIO(ASTPtr query)
{
    // TODO : try to use remote query executor and run it in cluster mode
    if (!std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(query))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTSelectWithUnionQuery is expected, but we get: {}", queryToString(query));
    }
#if 0
    auto select_interpreter = std::make_shared<InterpreterSelectWithUnionQuery>(query, context, options);
    auto block_io = std::make_shared<BlockIO>();
    return block_io;
#else
    auto cluster = context->getCluster(context->getSettings().distributed_shuffle_join_cluster.value)->getClusterWithReplicasAsShards(context->getSettingsRef());
    Pipes pipes;
    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    for (const auto & shard : cluster->getShardsAddresses())
    {
        for (const auto & node : shard)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name,
                node.port,
                context->getGlobalContext()->getCurrentDatabase(),
                node.user,
                node.password,
                node.cluster,
                node.cluster_secret,
                "InterpreterShufflePhasesSelectQuery RemoteInsert",
                node.compression,
                node.secure);

            String remote_query = queryToString(query);
            LOG_TRACE(&Poco::Logger::get("executeQuery"), "run on node:{}, query:{}", node.host_name, remote_query);
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection,
                remote_query,
                result_header,
                context,
                nullptr,
                scalars,
                Tables(),
                QueryProcessingStage::FetchColumns,
                RemoteQueryExecutor::Extension{});
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false));
        }
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(std::move(pipe));
    auto res = std::make_shared<BlockIO>();
    res->pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    return res;

#endif
}
}
