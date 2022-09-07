#include <memory>
#include <Interpreters/ASTRewriters/StageQueryShuffleFinishEventRewriteAction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTStageQuery.h>
#include <Parsers/queryToString.h>
#include <TableFunctions/TableFunctionShuffle.h>
#include <Common/logger_useful.h>
#include <fmt/core.h>
namespace DB
{

void StageQueryShuffleFinishEventRewriteAction::beforeVisitChildren(const ASTPtr & ast)
{
    frames.pushFrame(ast);
}

void StageQueryShuffleFinishEventRewriteAction::afterVisitChild(const ASTPtr & /*ast*/)
{
    auto child_result = frames.getTopFrame()->result_ast;
    frames.popFrame();
    frames.getTopFrame()->children_results.emplace_back(child_result);
}

ASTs StageQueryShuffleFinishEventRewriteAction::collectChildren(const ASTPtr & ast)
{
    if (!ast)
        return {};
    ASTs children;
    if (const auto * stage_ast = ast->as<ASTStageQuery>())
    {
        children.emplace_back(stage_ast->current_query);
        children.insert(children.end(), stage_ast->upstream_queries.begin(), stage_ast->upstream_queries.end());
    }
    return children;
}

void StageQueryShuffleFinishEventRewriteAction::visit(const ASTPtr & ast)
{
    if (const auto * stage_ast = ast->as<ASTStageQuery>())
    {
        visit(stage_ast);
    }
    else if (const auto * insert_ast = ast->as<ASTInsertQuery>())
    {
        visit(insert_ast);
    }
    else
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = frame->original_ast->clone();
    }
}

void StageQueryShuffleFinishEventRewriteAction::visit(const ASTStageQuery * /*stage_ast*/)
{
    auto frame = frames.getTopFrame();
    auto current_query = frame->children_results[0];
    ASTs upstream_queries;
    if (frame->children_results.size() > 1)
    {
        upstream_queries.insert(upstream_queries.end(), frame->children_results.begin() + 1, frame->children_results.end());
    }

    frame->result_ast = ASTStageQuery::make(current_query, upstream_queries);
}

void StageQueryShuffleFinishEventRewriteAction::visit(const ASTInsertQuery * insert_ast)
{
    auto frame = frames.getTopFrame();
    if (!insert_ast->table_function)
    {
        frame->result_ast = frame->original_ast->clone();
        return;
    }

    auto * table_function = insert_ast->table_function->as<ASTFunction>();
    auto & function_name = table_function->name;
    if (function_name == TableFunctionShuffleJoin::name || function_name == TableFunctionShuffleAggregation::name
        || function_name == TableFunctionLocalShuffle::name)
    {
        auto & args = table_function->arguments;
        auto cluster_name = args->children[0]->as<ASTLiteral>()->value.safeGet<String>();
        auto session_id = args->children[1]->as<ASTLiteral>()->value.safeGet<String>();
        auto table_id = args->children[2]->as<ASTLiteral>()->value.safeGet<String>();

        auto event_table_func = std::make_shared<ASTFunction>();
        event_table_func->name = TableFunctionClosedShuffle::name;
        event_table_func->arguments = std::make_shared<ASTExpressionList>();
        event_table_func->arguments->children.push_back(args->children[0]); // cluster name
        event_table_func->arguments->children.push_back(args->children[1]); // session id
        event_table_func->arguments->children.push_back(args->children[2]); // table id

        auto event_insert_query = std::make_shared<ASTInsertQuery>();
        event_insert_query->table_function = event_table_func;
        frame->result_ast = ASTStageQuery::make(event_insert_query, {insert_ast->clone()});
    }
    else
    {
        frame->result_ast = frame->original_ast->clone();
    }
}

}
