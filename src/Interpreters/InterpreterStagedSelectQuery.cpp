#include <memory>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterStagedSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTStagedSelectQuery.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/StagedJoinSelectStep.h>
#include <Common/ErrorCodes.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
#define MLOGGER &Poco::Logger::get("InterpreterStagedSelectQuery")
using BlockIOPtr = std::shared_ptr<BlockIO>;
using BlockIOPtrs = std::vector<BlockIOPtr>;
InterpreterStagedSelectQuery::InterpreterStagedSelectQuery(ASTPtr query_, ContextPtr context_, const SelectQueryOptions & options_):
    IInterpreterUnionOrSelectQuery(query_, context_, options_)
{
    initSampleBlock();
}

BlockIO InterpreterStagedSelectQuery::execute()
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

void InterpreterStagedSelectQuery::initSampleBlock()
{
    auto select_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(getSelectQuery()->final_select_query);
    if (!select_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTSelectWithUnionQuery, but we get : {}", queryToString(getSelectQuery()->final_select_query));
    result_header = InterpreterSelectWithUnionQuery(select_query, context, options).getSampleBlock();
}
void InterpreterStagedSelectQuery::ignoreWithTotals()
{
    auto select_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(getSelectQuery()->final_select_query);
    if (!select_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTSelectWithUnionQuery, but we get : {}", queryToString(getSelectQuery()->final_select_query));
    result_header = InterpreterSelectWithUnionQuery(select_query, context, options).getSampleBlock();

}

void InterpreterStagedSelectQuery::buildQueryPlan(QueryPlan & query_plan)
{
    auto staged_select_query = getSelectQuery();
    std::shared_ptr<BlockIOPtrs> insert_block_ios = std::make_shared<BlockIOPtrs>();
    int count = 0;
    for (auto & insert_ast : staged_select_query->staged_insert_queries)
    {
        if (!std::dynamic_pointer_cast<ASTInsertQuery>(insert_ast))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTInsertQuery is expected, but we get: {}", queryToString(insert_ast));

        auto insert_interprester = std::make_unique<InterpreterInsertQuery>(insert_ast, context);
        auto block_io = std::make_shared<BlockIO>(insert_interprester->execute());
        insert_block_ios->emplace_back(block_io);
        LOG_TRACE(MLOGGER, "add block io. {}", queryToString(insert_ast));
        count++;
        if (count > 10)
            break;
    }

    auto select_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(staged_select_query->final_select_query);
    if (!select_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTSelectWithUnionQuery, but we get : {}", queryToString(staged_select_query->final_select_query));
    
    std::make_unique<InterpreterSelectWithUnionQuery>(select_query, context, options)->buildQueryPlan(query_plan);
    query_plan.addStep(std::make_unique<StagedJoinSelectStep>(query_plan.getCurrentDataStream(), insert_block_ios));
     
}
}
