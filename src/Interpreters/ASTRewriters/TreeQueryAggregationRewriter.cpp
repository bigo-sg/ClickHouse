#include <cassert>
#include <memory>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/ASTRewriters/CollectRequiredColumnsVisitor.h>
#include <Interpreters/ASTRewriters/TreeQueryAggregationRewriter.h>
#include <Interpreters/ASTRewriters/TreeQueryJoinRewriter.h>
#include <Interpreters/ASTRewriters/UnresolveIdentifierTableRewriter.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTTreeQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <TableFunctions/TableFunctionShuffleJoin.h>
#include <base/logger_useful.h>
#include <base/types.h>
#include <Common/Exception.h>

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
    // If we have run EliminateCompositeColumnNameRewriter before, this should not happen 
    if ((ASTAnalyzeUtil::hasAggregationColumn(query) || ASTAnalyzeUtil::hasGroupBy(query)) && query->join())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is join in the SELECT with aggregation columns. query: {}", queryToString(*query));
    }

    const auto * joined_element  = query->join();
    if (joined_element)
    {
        return visitSelectWithJoin(query, depth, children);
    }

    if (ASTAnalyzeUtil::hasAggregationColumn(query) || ASTAnalyzeUtil::hasGroupBy(query))
    {
        if (!ASTAnalyzeUtil::hasGroupBy(query))
        {
            return visitOnlyAggregation(query, depth, children);
        }

        return visitGroupBy(query, depth, children);
    }
    
    auto left_table_expression = extractTableExpression(*query, 0);
    auto select_query_ref = query->clone();
    auto * select_query = select_query_ref->as<ASTSelectQuery>();
    if (left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        auto new_left_table_expression = visit(left_table_expression->as<ASTSelectWithUnionQuery>(), depth + 1, children);
        auto table_expr = std::make_shared<ASTTableExpression>();
        table_expr->subquery = std::make_shared<ASTSubquery>();
        table_expr->subquery->children.push_back(new_left_table_expression);
        table_expr->subquery->as<ASTSubquery>()->alias = ASTBuildUtil::getTableExpressionAlias(getTableExpression(*query, 0));

        auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
        table_element->children.push_back(table_expr);
        table_element->table_expression = table_expr;

        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto tables = select_query->tables();
        tables->children.push_back(table_element);
    }

    if (depth)
        return select_query_ref;

    return ASTTreeQuery::make(select_query_ref, children);
}

ASTPtr TreeQueryAggregationRewriter::visitSelectWithJoin(ASTSelectQuery * query, UInt32 depth, ASTs & children)
{
    TreeQueryJoinRewriter join_rewriter(context, query->clone());
    auto new_join_select = join_rewriter.run();
    auto * tree_query = new_join_select->as<ASTTreeQuery>();
    if (!tree_query)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTTreeQuery is expected, but we get {}", new_join_select->getID());
    }

    if (depth)
    {
        for (auto & child : tree_query->input_asts)
        {
            children.push_back(child);
        }
        return tree_query->output_ast;
    }
    return new_join_select;
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

ASTPtr TreeQueryAggregationRewriter::visitGroupBy(ASTSelectQuery * query, UInt32 depth, ASTs & children)
{
    // visit child first
    //LOG_TRACE(logger, "visit group by query:{}", queryToString(*query));
    auto depth_visited_select_query_ref = query->clone();
    auto * depth_visited_select_query = depth_visited_select_query_ref->as<ASTSelectQuery>();
    ASTs sub_children;
    auto new_table_expression_ref = getTableExpression(*query, 0)->clone();
    auto * new_table_expression = new_table_expression_ref->as<ASTTableExpression>();
    if (new_table_expression->subquery)
    {
        auto new_subquery = visitChild(new_table_expression->subquery->children[0].get(), depth + 1, sub_children);
        ASTBuildUtil::updateSelectLeftTableBySubquery(
            depth_visited_select_query,
            new_subquery->as<ASTSelectWithUnionQuery>(),
            ASTBuildUtil::getTableExpressionAlias(new_table_expression));
        //LOG_TRACE(logger, "update depth_visited_select_query: {}", queryToString(*depth_visited_select_query));
    }

    auto tables = getDatabaseAndTablesWithColumns(getTableExpressions(*depth_visited_select_query), context, true, true);
    assert(tables.size() == 1);
    std::vector<ColumnWithDetailNameAndTypes> required_columns = {ColumnWithDetailNameAndTypes{}};
    CollectRequiredColumnsMatcher::Data visit_data = {.tables = tables, .required_columns = required_columns};
    CollectRequiredColumnsVisitor(visit_data).visit(query->clone());

    auto insert_query = makeInsertSelectQuery(
        required_columns[0], depth_visited_select_query, depth_visited_select_query->groupBy()->as<ASTExpressionList>());
    
    auto table_function = insert_query->as<ASTInsertQuery>()->table_function->clone();
    ASTBuildUtil::updateSelectLeftTableByTableFunction(depth_visited_select_query, table_function->as<ASTFunction>(), ASTBuildUtil::getTableExpressionAlias(new_table_expression));
    if (!sub_children.empty())
    {
        children.push_back(ASTTreeQuery::make(insert_query, sub_children));
    }
    else
        children.push_back(insert_query);

    if (depth)
        return depth_visited_select_query_ref;
    auto res =  ASTTreeQuery::make(depth_visited_select_query_ref, children);
    children.clear();
    return res;
}

ASTPtr TreeQueryAggregationRewriter::visitOnlyAggregation(ASTSelectQuery * query, UInt32 /*depth*/, ASTs & /*children*/)
{
    // don't change it
    return query->clone();
}

ASTPtr TreeQueryAggregationRewriter::makeInsertSelectQuery(
    const ColumnWithDetailNameAndTypes & required_columns, const ASTSelectQuery * original_select_query, const ASTExpressionList * groupby)
{
    auto session_id = context->getClientInfo().current_query_id;
    auto table_id = getNextId();
    auto table_structure = ASTBuildUtil::toTableStructureDescription(required_columns);
    String hash_expression_list;
    if (groupby)
    {
        UnresolveIdentifierTableRewriter indent_rewriter(context, groupby->clone());
        hash_expression_list = queryToString(indent_rewriter.run());
    }

    auto insert_table_function = makeInsertTableFunction(session_id, table_id, table_structure, hash_expression_list);

    auto select_query_ref = original_select_query->clone();
    auto * select_query = select_query_ref->as<ASTSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, nullptr);
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, ASTBuildUtil::toShortNameExpressionList(required_columns));
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
    select_with_union_query->list_of_selects->children.push_back(select_query_ref);

    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->table_function = insert_table_function;
    insert_query->select = select_with_union_query;
    return insert_query;
}


ASTPtr TreeQueryAggregationRewriter::makeInsertTableFunction(
    const String & session_id, const String & id, const String & table_structure, const String & hash_expression_list)
{
    auto table_function = std::make_shared<ASTFunction>();
    table_function->name = TableFunctionShuffleAggregation::name;

    table_function->arguments = std::make_shared<ASTExpressionList>();
    auto * table_args = table_function->arguments->as<ASTExpressionList>();
    table_function->children.push_back(table_function->arguments);

    String cluster_name = context->getSettings().distributed_shuffle_cluster.value;
    table_args->children.push_back(std::make_shared<ASTLiteral>(cluster_name));
    table_args->children.push_back(std::make_shared<ASTLiteral>(session_id));
    table_args->children.push_back(std::make_shared<ASTLiteral>(id));
    table_args->children.push_back(std::make_shared<ASTLiteral>(table_structure));
    table_args->children.push_back(std::make_shared<ASTLiteral>(hash_expression_list));

    return table_function;
}
}
