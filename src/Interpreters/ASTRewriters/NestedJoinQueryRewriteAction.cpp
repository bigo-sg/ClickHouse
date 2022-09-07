#include <memory>
#include <vector>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/ASTRewriters/ASTBuildUtil.h>
#include <Interpreters/ASTRewriters/CollectRequiredColumnsAction.h>
#include <Interpreters/ASTRewriters/IdentRenameRewriteAction.h>
#include <Interpreters/ASTRewriters/NestedJoinQueryRewriteAction.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void NestedJoinQueryRewriteAction::beforeVisitChildren(const ASTPtr & ast)
{
    frames.pushFrame(ast);
}

void NestedJoinQueryRewriteAction::afterVisitChild(const ASTPtr & /*ast*/)
{
    auto child_result = frames.getTopFrame()->result_ast;
    frames.popFrame();
    assert(frames.size() != 0);
    frames.getTopFrame()->children_results.emplace_back(child_result);
}

ASTs NestedJoinQueryRewriteAction::collectChildren(const ASTPtr & ast)
{

    ASTs children;
    if (!ast)
        return children;

    if (const auto * table_expr_ast = ast->as<ASTTableExpression>())
    {
        if (table_expr_ast->subquery)
            children.emplace_back(table_expr_ast->subquery);
    }
    else if (const auto * subquery_ast = ast->as<ASTSubquery>())
    {
        children.emplace_back(subquery_ast->children[0]);
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        if (const auto * left_table_expr = getTableExpression(*select_ast, 0))
            children.emplace_back(left_table_expr->clone());
        if (const auto * right_table_expr = getTableExpression(*select_ast, 1))
            children.emplace_back(right_table_expr->clone());
    }
    else if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        children = select_with_union_ast->list_of_selects->children;
    }
    return children;
}

void NestedJoinQueryRewriteAction::visit(const ASTPtr & ast)
{
    if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        visit(select_with_union_ast);
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        visit(select_ast);
    }
    else if (const auto * table_expr_ast = ast->as<ASTTableExpression>())
    {
        visit(table_expr_ast);
    }
    else if (const auto * subquery_ast = ast->as<ASTSubquery>())
    {
        visit(subquery_ast);
    }
    else
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = frame->original_ast;
    }
}

void NestedJoinQueryRewriteAction::visit(const ASTSelectWithUnionQuery * select_with_union_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = select_with_union_ast->clone();
    auto * result_select_ast = frame->result_ast->as<ASTSelectWithUnionQuery>();
    result_select_ast->list_of_selects->children = frame->children_results;
}

void NestedJoinQueryRewriteAction::visit(const ASTSelectQuery * select_ast)
{
    auto frame = frames.getTopFrame();
    if (frame->children_results.size() == 1)
    {
        frame->result_ast = select_ast->clone();
        auto * result_select_ast = frame->result_ast->as<ASTSelectQuery>();
        auto * table_expr_ast = frame->children_results[0]->as<ASTTableExpression>();
        if (table_expr_ast->subquery)
        {
            auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*select_ast), context, true, true);
            String table_alias = tables_with_columns[0].table.alias;
            if (table_alias.empty() && !tables_with_columns[0].table.table.empty())
                table_alias = tables_with_columns[0].table.table;

            table_expr_ast->subquery->as<ASTSubquery>()->setAlias(table_alias);
        }
        auto table_element = ASTBuildUtil::createTablesInSelectQueryElement(table_expr_ast);
        ASTBuildUtil::updateSelectQueryTables(
            result_select_ast, table_element->as<ASTTablesInSelectQueryElement>());
        renameSelectQueryIdentifiers(result_select_ast);
    }
    else if (frame->children_results.size() == 2)
    {
        //auto nested_select_ast_ref = select_ast->clone();
        auto nested_select_ast_ref = std::make_shared<ASTSelectQuery>();
        auto * nested_select_ast = nested_select_ast_ref->as<ASTSelectQuery>();
        ASTBuildUtil::updateSelectQueryTables(
            nested_select_ast,
            ASTBuildUtil::createTablesInSelectQueryElement(frame->children_results[0]->as<ASTTableExpression>())
                ->as<ASTTablesInSelectQueryElement>(),
            ASTBuildUtil::createTablesInSelectQueryElement(
                frame->children_results[1]->as<ASTTableExpression>(), select_ast->join()->table_join)
                ->as<ASTTablesInSelectQueryElement>());
        auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*select_ast), context, true, true);
        CollectRequiredColumnsAction action(tables_with_columns);
        ASTDepthFirstVisitor<CollectRequiredColumnsAction> visitor(action, select_ast->clone());
        auto tables_required_cols = visitor.visit().required_columns;
        auto & right_required_cols = tables_required_cols[1];
        ColumnWithDetailNameAndType::makeAliasByFullName(right_required_cols);

        auto nested_select_expr_list = std::make_shared<ASTExpressionList>();
        for (const auto & col : tables_required_cols[0])
        {
            auto ident = std::make_shared<ASTIdentifier>(col.splitedFullName());
            if (!col.alias_name.empty())
                ident->alias = col.alias_name;
            else if (!col.short_name.empty() && col.short_name != col.full_name)
                ident->alias = col.short_name;
            nested_select_expr_list->children.emplace_back(ident);
        }
        for (const auto & col : tables_required_cols[1])
        {
            auto ident = std::make_shared<ASTIdentifier>(col.splitedFullName());
            ident->alias = col.alias_name;
            //if (ident->alias.empty())
            //    throw Exception(ErrorCodes::LOGICAL_ERROR, "Alias is expected for {}", col.full_name);
            nested_select_expr_list->children.emplace_back(ident);
        }
        nested_select_ast->setExpression(ASTSelectQuery::Expression::SELECT, nested_select_expr_list);
        if (auto where_expr = select_ast->where())
            nested_select_ast->setExpression(ASTSelectQuery::Expression::WHERE, where_expr->clone());
        // replace all columns with the renamed map from inner sub-queries
        renameSelectQueryIdentifiers(nested_select_ast);

        // add new columns to be renamed for the outside queries.
        for (const auto & col : right_required_cols)
        {
            if (col.full_name != col.short_name)
            {
                if (!columns_alias.count(col.full_name))
                {
                    columns_alias[col.full_name] = col.alias_name;
                }
            }
        }

        frame->result_ast = select_ast->clone();
        auto * result_select_ast = frame->result_ast->as<ASTSelectQuery>();
        auto select_with_union_ast = std::make_shared<ASTSelectWithUnionQuery>();
        select_with_union_ast->list_of_selects = std::make_shared<ASTExpressionList>();
        select_with_union_ast->list_of_selects->children.emplace_back(nested_select_ast_ref);
        String table_alias = tables_with_columns[0].table.alias;
        if (table_alias.empty() && !tables_with_columns[0].table.table.empty())
        {
            table_alias = tables_with_columns[0].table.table;
        }
        ASTBuildUtil::updateSelectQueryTables(
            result_select_ast,
            ASTBuildUtil::createTablesInSelectQueryElement(select_with_union_ast.get(), table_alias)->as<ASTTablesInSelectQueryElement>());

        renameSelectQueryIdentifiers(result_select_ast);
    }

    clearRenameAlias(select_ast);
}

void NestedJoinQueryRewriteAction::visit(const ASTTableExpression * table_expr_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = table_expr_ast->clone();
    auto * result_table_expr_ast = frame->result_ast->as<ASTTableExpression>();
    if (!frame->children_results.empty())
        result_table_expr_ast->subquery = frame->children_results[0];
}

void NestedJoinQueryRewriteAction::visit(const ASTSubquery * subquery_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = subquery_ast->clone();
    auto * result_subquery_ast = frame->result_ast->as<ASTSubquery>();
    result_subquery_ast->children = frame->children_results;
}

void NestedJoinQueryRewriteAction::updateIdentNames(ASTSelectQuery * ast, ASTSelectQuery::Expression index)
{
    auto expression = ast->getExpression(index, false);
    if (!expression)
        return;
    IdentifierRenameAction action(context, columns_alias);
    ASTDepthFirstVisitor<IdentifierRenameAction> visitor(action, expression);
    auto result = visitor.visit();
    ast->setExpression(index, std::move(result));
}

void NestedJoinQueryRewriteAction::renameSelectQueryIdentifiers(ASTSelectQuery * select_ast)
{
    updateIdentNames(select_ast, ASTSelectQuery::Expression::SELECT);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::WHERE);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::GROUP_BY);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::ORDER_BY);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::PREWHERE);

    if (select_ast->join())
    {
        auto * join = select_ast->join()->table_join->as<ASTTableJoin>();
        if (join->using_expression_list)
        {
            IdentifierRenameAction action(context, columns_alias);
            ASTDepthFirstVisitor<IdentifierRenameAction> visitor(action, join->using_expression_list);
            join->using_expression_list = visitor.visit();
        }
        if (join->on_expression)
        {
            IdentifierRenameAction action(context, columns_alias);
            ASTDepthFirstVisitor<IdentifierRenameAction> visitor(action, join->on_expression);
            join->on_expression = visitor.visit();
        }
    }
}
void NestedJoinQueryRewriteAction::clearRenameAlias(const ASTSelectQuery * select_ast)
{
    auto re_alias = collectAliasColumns(select_ast->select()->as<ASTExpressionList>());
    for (const auto & alias : re_alias)
    {
        columns_alias.erase(alias.first);
    }
}

std::map<String, String> NestedJoinQueryRewriteAction::collectAliasColumns(const ASTExpressionList * select_expression)
{
    std::map<String, String> res;
    for (const auto & child : select_expression->children)
    {
        if (auto * ident = child->as<ASTIdentifier>())
        {
            auto alias = ident->tryGetAlias();
            if (!alias.empty())
            {
                res[ident->name()] = alias;
            }
        }
    }
    return res;
}
}
