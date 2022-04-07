#include <memory>
#include <Interpreters/ASTRewriters/TreeQueryJoinRewriter.h>
#include <Parsers/IAST_fwd.h>
#include <algorithm>
#include <memory>
#include <ucontext.h>
#include <Interpreters/CollectJoinColumnsVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/ASTRewriters/TreeDistributedShuffleJoinRewriter.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTShufflePhasesSelectQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Storages/IStorage_fwd.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>
#include "Interpreters/ASTRewriters/ASTAnalyzeUtil.h"
#include "Interpreters/ASTRewriters/TreeQueryAggregationRewriter.h"
#include "Interpreters/Context_fwd.h"
#include "Parsers/ASTTreeQuery.h"
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSubquery.h>
#include <boost/math/tools/roots.hpp>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/ASTRewriters/TreeQueryAggregationRewriter.h>

namespace DB
{

TreeQueryJoinRewriter::TreeQueryJoinRewriter(ContextPtr context_, ASTPtr ast_) : context(context_), from_ast(ast_)
{
}
ASTPtr TreeQueryJoinRewriter::run()
{
    ASTPtr res;
    ASTs children;
    res = visitChild(from_ast.get(), 0, children);
    if (!res)
        return ASTTreeQuery::make(from_ast, {});
    if(res->getID() != "ASTTreeQeury")
        return ASTTreeQuery::make(res, {});
    return res;
}

ASTPtr TreeQueryJoinRewriter::visitChild(IAST * ast, size_t depth, ASTs & children)
{
    if (auto * select_ast = ast->as<ASTSelectQuery>())
    {
        return visit(select_ast, depth, children);
    }
    else if (auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        return visit(select_with_union_ast, depth, children);
    }
    return ast->clone();
}
ASTPtr TreeQueryJoinRewriter::visit(ASTSelectWithUnionQuery * ast, size_t depth, ASTs & children)
{
    auto select_ast_ref = ast->clone();
    auto * select_ast = select_ast_ref->as<ASTSelectWithUnionQuery>();
    select_ast->list_of_selects->children.clear();
    for(auto & child : ast->list_of_selects->children)
    {
        auto new_child = visitChild(child.get(), depth + 1, children);
        if (!new_child)
            return nullptr;
        select_ast->list_of_selects->children.push_back(new_child);
    }

    if (depth)
        return select_ast_ref;

    auto tree_ast = ASTTreeQuery::make(select_ast_ref, children);
    children.clear();
    return tree_ast;
}

ASTPtr TreeQueryJoinRewriter::visit(ASTSelectQuery * ast, size_t depth, ASTs & children)
{
    // If we have run EliminateCompositeColumnNameRewriter before, this should not happen 
    if ((ASTAnalyzeUtil::hasAggregationColumn(ast) || ASTAnalyzeUtil::hasGroupBy(ast)) && ast->join())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is join in the SELECT with aggregation columns. query: {}", queryToString(*ast));
    }

    const auto * joined_element = ast->join();
    if (joined_element)
    {
        return visitJoinedSelect(ast, depth, children);
    }

    if (ASTAnalyzeUtil::hasGroupBy(ast))
    {
        TreeQueryAggregationRewriter rewriter(context, ast->clone());
        auto rewritten_select_query = rewriter.run();
        auto * tree_query = rewritten_select_query->as<ASTTreeQuery>();
        if (!tree_query)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTTreeQuery is expected, but we get {}", rewritten_select_query->getID());
        }
        if (depth)
        {
            for (auto & child : tree_query->input_asts)
            {
                children.push_back(child);
            }
            return tree_query->output_ast;
        }
        return rewritten_select_query;
    }
    auto left_table_expression = extractTableExpression(*ast, 0);
    if (left_table_expression && left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        auto * left_subquery = left_table_expression->as<ASTSelectWithUnionQuery>();
        auto new_left_subquery = visit(left_subquery, depth + 1, children);
        if (!new_left_subquery)
            return nullptr;
        auto table_alias = ASTBuildUtil::getTableExpressionAlias(getTableExpression(*ast, 0));
        auto new_select_ast_ref = ast->clone();
        auto * new_select_ast = new_select_ast_ref->as<ASTSelectQuery>();
        ASTBuildUtil::updateSelectLeftTableBySubquery(new_select_ast, new_left_subquery->as<ASTSelectWithUnionQuery>(), table_alias);

        if (depth)
            return new_select_ast_ref;
        
        // Only when we have join actions on subquery, we rebuild a ASTTreeQuery
        auto res = ASTTreeQuery::make(new_select_ast_ref, children);
        children.clear();
        return res;
    }

    if (depth)
        return ast->clone();
    return ASTTreeQuery::make(ast->clone(), {});
}



ASTPtr TreeQueryJoinRewriter::visitJoinedSelect(ASTSelectQuery * ast, size_t depth, ASTs & children)
{
    TreeDistributedShuffleJoinRewriter rewriter(std::dynamic_pointer_cast<ASTSelectQuery>(ast->clone()), context, getNextTableId());
    if (!rewriter.prepare())
    {
        return ast->clone();
    }

    ASTs left_children, right_children;
    auto new_left_table_expr = visitJoinedTableExpression(getTableExpression(*ast, 0), left_children);
    if (!new_left_table_expr)
        return nullptr;
    auto new_right_table_expr = visitJoinedTableExpression(getTableExpression(*ast, 1), right_children);
    if (!new_right_table_expr)
        return nullptr;
    auto left_table_id = rewriter.getHashTableId("left");
    auto right_table_id = rewriter.getHashTableId("right");
    ASTPtr left_insert_query = rewriter.createSubJoinTable(left_table_id, new_left_table_expr, 0);
    ASTPtr right_insert_query = rewriter.createSubJoinTable(right_table_id, new_right_table_expr, 1);
    if (!left_children.empty())
        left_insert_query = ASTTreeQuery::make(left_insert_query, left_children);
    if (!right_children.empty())
        right_insert_query = ASTTreeQuery::make(right_insert_query, right_children);

    auto new_query = rewriter.createNewJoinSelectQuery(left_table_id, right_table_id);

    children.emplace_back(left_insert_query);
    children.emplace_back(right_insert_query);
    LOG_TRACE(logger, "new join select query:{}", queryToString(new_query));
    LOG_TRACE(logger, "new join select left query:{}", queryToString(left_insert_query));
    LOG_TRACE(logger, "new join select right query:{}", queryToString(right_insert_query));
    if (depth)
    {
        return new_query;
    }
    
    new_query = ASTTreeQuery::make(new_query, children);
    children.clear();
    return new_query;

}

std::shared_ptr<ASTTableExpression> TreeQueryJoinRewriter::visitJoinedTableExpression(const ASTTableExpression * table_expression, ASTs & children)
{
    std::shared_ptr<ASTTableExpression> new_table_expression;
    if (table_expression->database_and_table_name || table_expression->table_function)
    {
        new_table_expression = std::dynamic_pointer_cast<ASTTableExpression>(table_expression->clone());
    }
    else if (table_expression->subquery)
    {
        auto slect_query_with_union_ref = table_expression->subquery->children[0]->clone();
        auto * slect_query_with_union = slect_query_with_union_ref->as<ASTSelectWithUnionQuery>();
        auto new_left_subquery = visit(slect_query_with_union, 1, children);
        new_table_expression = std::make_shared<ASTTableExpression>();
        auto subquery = std::make_shared<ASTSubquery>();
        subquery->children.push_back(new_left_subquery);
        subquery->setAlias(ASTBuildUtil::getTableExpressionAlias(table_expression));   
        new_table_expression->subquery = subquery;
    }

    return new_table_expression;
}

}
