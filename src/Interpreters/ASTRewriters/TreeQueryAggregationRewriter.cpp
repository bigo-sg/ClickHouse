#include <cassert>
#include <memory>
#include <Interpreters/ASTRewriters/TreeQueryAggregationRewriter.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTTreeQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/getTableExpressions.h>
#include <base/types.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TreeQueryAggregationRewriter::TreeQueryAggregationRewriter(ContextPtr context_, ASTPtr ast_)
    : context(context_)
    , from_ast(ast_)
{
}

ASTPtr TreeQueryAggregationRewriter::run()
{
    ASTs children;
    auto res = visitChild(from_ast.get(), 0, children);
    return res;
}

ASTPtr TreeQueryAggregationRewriter::visitChild(IAST * query, UInt32 depth, ASTs & children)
{
    if (auto * select_query = query->as<ASTSelectQuery>())
    {
        return visit(select_query, depth, children);
    }
    else if (auto * select_with_union_query = query->as<ASTSelectWithUnionQuery>())
    {
        return visit(select_with_union_query, depth, children);
    }
    else if (auto * insert_query = query->as<ASTInsertQuery>())
    {
        return visit(insert_query, depth, children);
    }
    else if (auto * tree_query = query->as<ASTTreeQuery>())
    {
        return visit(tree_query, depth, children);
    }
    return query->clone();
}

ASTPtr TreeQueryAggregationRewriter::visit(ASTSelectQuery * query, UInt32 depth, ASTs & children)
{
    const auto * joined_element  = query->join();
    if (joined_element)
    {
        return visitSelectWithJoin(query, depth, children);
    }
    #if 0
    auto left_table_expression = extractTableExpression(*query, 0);
    ASTPtr new_left_table_expression;
    ASTPtr shuffle_child_ast;
    auto cloned_query = query->clone();
    if (left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        new_left_table_expression = visit(left_table_expression->as<ASTSelectWithUnionQuery>(), data, depth + 1, children);
    }
    else if (left_table_expression->as<ASTFunction>())
    {
        new_left_table_expression = left_table_expression;
    }
    else if (left_table_expression->as<ASTTableIdentifier>())
    {
        new_left_table_expression = left_table_expression;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknow ast type: {}", left_table_expression->getID());    
    }
    if (query->groupBy())
    {
        auto res = makeAggreationShuffleTreeQuery(new_left_table_expression, cloned_query, data);
        new_left_table_expression = res.first;
        shuffle_child_ast = res.second;
    }
    if (shuffle_child_ast && !children.empty())
    {
        ASTs tmp_children;
        tmp_children.swap(children);
        children.emplace_back(ASTTreeQuery::make(shuffle_child_ast, tmp_children));
    }
    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->subquery = std::make_shared<ASTSubquery>();
    table_expr->subquery->children.push_back(new_left_table_expression);

    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expr);
    table_element->table_expression = table_expr;

    auto new_select_query = std::dynamic_pointer_cast<ASTSelectQuery>(cloned_query);
    new_select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = new_select_query->tables();
    tables->children.push_back(table_element);

    if (depth || children.empty())
        return new_select_query;

    return ASTTreeQuery::make(new_select_query, children);
    #else
    // currently we don't handle this case
    auto left_table_expression = extractTableExpression(*query, 0);
    ASTPtr new_left_table_expression;
    ASTPtr shuffle_child_ast;
    auto cloned_query = query->clone();
    if (left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        new_left_table_expression = visit(left_table_expression->as<ASTSelectWithUnionQuery>(), depth + 1, children);
    }
    else if (left_table_expression->as<ASTFunction>())
    {
        new_left_table_expression = left_table_expression;
    }
    else if (left_table_expression->as<ASTTableIdentifier>())
    {
        new_left_table_expression = left_table_expression;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknow ast type: {}", left_table_expression->getID());    
    }

    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->subquery = std::make_shared<ASTSubquery>();
    table_expr->subquery->children.push_back(new_left_table_expression);

    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expr);
    table_element->table_expression = table_expr;

    auto new_select_query = std::dynamic_pointer_cast<ASTSelectQuery>(cloned_query);
    new_select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = new_select_query->tables();
    tables->children.push_back(table_element);

    if (depth || children.empty())
        return new_select_query;

    return ASTTreeQuery::make(new_select_query, children);
    #endif
}

ASTPtr TreeQueryAggregationRewriter::visitSelectWithJoin(ASTSelectQuery * /*query*/, UInt32 /*depth*/, ASTs & /*children*/)
{
    return nullptr;
}

ASTPtr TreeQueryAggregationRewriter::visit(ASTSelectWithUnionQuery * query, UInt32 depth, ASTs & children)
{
    ASTs union_selects;
    for (auto & child : query->list_of_selects->children)
    {
        auto select = visitChild(child.get(), depth + 1, children);
        union_selects.emplace_back(select);
    }
    auto res = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(query->clone());
    res->list_of_selects->children.swap(union_selects);
    if (depth || children.empty())
        return res;
    return ASTTreeQuery::make(res, children);
}
ASTPtr TreeQueryAggregationRewriter::visit(ASTInsertQuery * query, UInt32 depth, ASTs & children)
{
    assert(!depth);
    auto select_ast = visitChild(query->select.get(), depth + 1, children);
    auto res = std::dynamic_pointer_cast<ASTInsertQuery>(query->clone());
    res->select = select_ast;
    if (children.empty())
        return res;
    return ASTTreeQuery::make(res, children);
}
ASTPtr TreeQueryAggregationRewriter::visit(ASTTreeQuery * query, UInt32 /*depth*/, ASTs & /*children*/)
{
    ASTs new_input_asts;
    for (auto & input_ast : query->input_asts)
    {
        ASTs input_children;
        auto res = visitChild(input_ast.get(), 0, input_children);
        new_input_asts.emplace_back(res);
    }
    
    ASTs output_children;
    auto new_output = visitChild(query->output_ast.get(), 0, output_children);
    return ASTTreeQuery::make(new_output, new_input_asts);
}
}
