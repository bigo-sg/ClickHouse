#include <memory>
#include <vector>
#include <Interpreters/ASTRewriters/EliminateCompositeColumnNameRewriter.h>
#include <base/logger_useful.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Interpreters/ASTRewriters/IdentRenameRewriter.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/ASTRewriters/CollectRequiredColumnsVisitor.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

EliminateCompositeColumnNameRewriter::EliminateCompositeColumnNameRewriter(ContextPtr context_, ASTPtr ast_)
    : context(context_)
    , from_ast(ast_)
{}

String EliminateCompositeColumnNameRewriter::getFunctionAlias(const String & function_name)
{
    auto iter = function_alias_id.find(function_name);
    if (iter == function_alias_id.end())
    {
        function_alias_id[function_name] = 0;
        return function_name + "0";
    }
    iter->second += 1;
    return function_name + std::to_string(iter->second);
}

ASTPtr EliminateCompositeColumnNameRewriter::run()
{
    auto res =  visitChild(from_ast.get());
    LOG_TRACE(logger, "result query: {}", queryToString(res));
    return res;
}

ASTPtr EliminateCompositeColumnNameRewriter::visitChild(IAST * ast)
{
    ASTPtr res;
    if (auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        res = visit(select_with_union_ast);
    }
    else if (auto * select_ast = ast->as<ASTSelectQuery>())
    {
        res = visit(select_ast);
    }
    else if (auto * insert_ast = ast->as<ASTInsertQuery>())
    {
        res = visit(insert_ast);
    }
    else 
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknow ast type: {}", ast->getID());    
    }
    return res;
}

ASTPtr EliminateCompositeColumnNameRewriter::visit(ASTSelectWithUnionQuery * ast)
{
    ASTs children;
    for (auto & child : ast->list_of_selects->children)
    {
        children.emplace_back(visitChild(child.get()));
    }
    auto clone_ast = ast->clone();
    auto * res = clone_ast->as<ASTSelectWithUnionQuery>();
    res->list_of_selects->children = children;
    return clone_ast;
}

std::map<String, String> EliminateCompositeColumnNameRewriter::collectAliasColumns(ASTExpressionList * select_expression)
{
    std::map<String, String> res;
    for (auto & child : select_expression->children)
    {
        if (auto * ident = child->as<ASTIdentifier>())
        {
            auto alias = ident->tryGetAlias();
            if (!alias.empty())
            {
                res[ident->name()] = alias;
                LOG_TRACE(logger, "Meet column alias: {} -> {}", ident->name(), alias);
            }
        }
    }
    return res;
}

void EliminateCompositeColumnNameRewriter::updateIdentNames(ASTSelectQuery * ast, ASTSelectQuery::Expression index, bool rename_function)
{
    auto expression = ast->getExpression(index, false);
    if (!expression)
        return;
    printColumnAlias();
    LOG_TRACE(logger, "rename expression {} idents. before:{}", index, queryToString(expression));
    IdentRenameRewriter select_rewriter(context, expression, columns_alias, function_alias_id, rename_function);
    auto select_expression = select_rewriter.run();
    LOG_TRACE(logger, "rename expression {} idents. after:{}", index, queryToString(select_expression));
    ast->setExpression(index, std::move(select_expression));
}
void EliminateCompositeColumnNameRewriter::renameSelectQueryIdentifiers(ASTSelectQuery * select_ast)
{
    updateIdentNames(select_ast, ASTSelectQuery::Expression::SELECT, true);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::WHERE);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::GROUP_BY);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::ORDER_BY);
    updateIdentNames(select_ast, ASTSelectQuery::Expression::PREWHERE);

    if (select_ast->join())
    {
        auto * join = select_ast->join()->table_join->as<ASTTableJoin>();
        if (join->using_expression_list)
        {
            IdentRenameRewriter rewriter(context, join->using_expression_list, columns_alias, function_alias_id, false);
            join->using_expression_list = rewriter.run();
        }
        if (join->on_expression)
        {
            IdentRenameRewriter rewriter(context, join->on_expression, columns_alias, function_alias_id, false);
            join->on_expression = rewriter.run();
        }
    }
    
}

void EliminateCompositeColumnNameRewriter::clearRenameAlias(ASTSelectQuery * select_ast)
{
    auto re_alias = collectAliasColumns(select_ast->select()->as<ASTExpressionList>());
    for (const auto & alias : re_alias)
    {
        columns_alias.erase(alias.first);
    }

}
ASTPtr EliminateCompositeColumnNameRewriter::visit(ASTSelectQuery * ast)
{
    const auto * joined_table = ast->join();
    if (joined_table)
    {
        return visitJoinedSelect(ast);
    }
    auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*ast), context, true, true);
    auto left_table_expression = extractTableExpression(*ast, 0);
    auto res = ast->clone();
    auto * select_ast = res->as<ASTSelectQuery>();
    if (auto * select_with_union_ast = left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        auto new_subquery = visit(select_with_union_ast);
        LOG_TRACE(logger, "to rename ASTSelectQuery:{}", queryToString(res));
        printColumnAlias();
        String table_alias = tables_with_columns[0].table.alias;
        if (table_alias.empty() && !tables_with_columns[0].table.table.empty())
        {
            table_alias = tables_with_columns[0].table.table;
        }
        ASTBuildUtil::updateSelectLeftTableBySubquery(
            res->as<ASTSelectQuery>(), new_subquery->as<ASTSelectWithUnionQuery>(), table_alias);
        renameSelectQueryIdentifiers(select_ast);
    }
    clearRenameAlias(ast);
    return res;
}

ASTPtr EliminateCompositeColumnNameRewriter::visitJoinedSelect(ASTSelectQuery *ast)
{
    auto left_table_expression = visitJoinedTableExpression(getTableExpression(*ast, 0));
    auto right_table_expression = visitJoinedTableExpression(getTableExpression(*ast, 1));
    auto children_visited_ast_ref = ast->clone();
    auto * children_visited_ast = children_visited_ast_ref->as<ASTSelectQuery>();
    auto join_ref = children_visited_ast->join()->table_join->clone();
    LOG_TRACE(logger, "visitJoinedSelect. ast={}", queryToString(*ast));
    LOG_TRACE(logger, "join ast id: {}", join_ref->getID());
    ASTBuildUtil::updateJoinedSelectTables(
        children_visited_ast,
        left_table_expression->as<ASTTableExpression>(),
        right_table_expression->as<ASTTableExpression>(),
        join_ref->as<ASTTableJoin>());
    renameSelectQueryIdentifiers(children_visited_ast);
    clearRenameAlias(ast);
    LOG_TRACE(logger, "1) children_visited_ast:{}", queryToString(children_visited_ast_ref));

    
    auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*children_visited_ast), context, true, true);
    std::vector<ColumnWithDetailNameAndTypes> tables_requried_columns{{},{}};
    CollectRequiredColumnsMatcher::Data data = {.tables = tables_with_columns, .required_columns=tables_requried_columns};
    CollectRequiredColumnsVisitor(data).visit(ast->clone());
    auto & right_required_columns = data.required_columns[1];
    ColumnWithDetailNameAndType::makeAliasByFullName(right_required_columns);
    for (const auto & col : data.required_columns[0])
    {
        LOG_TRACE(logger, "left_required_columns: {}", col.toString());
    }
    for (const auto & col : right_required_columns)
    {
        LOG_TRACE(logger, "right_required_columns. {}", col.toString());
        if (col.full_name != col.short_name)
        {
            if (columns_alias.count(col.full_name))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Confilict column name: {}", col.full_name);
            }
            columns_alias[col.full_name] = col.alias_name;
        }
    }
    renameSelectQueryIdentifiers(children_visited_ast);
    LOG_TRACE(logger, "2) children_visited_ast:{}", queryToString(children_visited_ast_ref));

    auto inner_select_ast = std::make_shared<ASTSelectQuery>();
    auto select_expression = std::make_shared<ASTExpressionList>();
    for (auto & col : data.required_columns[0])
    {
        auto ident = std::make_shared<ASTIdentifier>(col.splitedFullName());
        if (!col.alias_name.empty())
            ident->alias = col.alias_name;
        else if (!col.short_name.empty() && col.short_name != col.full_name)
            ident->alias = col.short_name;
        select_expression->children.push_back(ident);
    }
    for (auto & col : data.required_columns[1])
    {
        auto ident = std::make_shared<ASTIdentifier>(col.splitedFullName());
        if (!col.alias_name.empty() && col.full_name != col.alias_name)
        {
            ident->alias = col.alias_name;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Alias name is expected for {}", col.full_name);
        select_expression->children.push_back(ident);
    }
    LOG_TRACE(logger, "build select expression list:{}", queryToString(select_expression));
    inner_select_ast->setExpression(ASTSelectQuery::Expression::SELECT, select_expression);
    auto inner_left_table_expr = getTableExpression(*children_visited_ast, 0)->clone();
    auto inner_right_table_expr = getTableExpression(*children_visited_ast, 1)->clone();
    auto inner_table_join = children_visited_ast->join()->table_join;
    ASTBuildUtil::updateJoinedSelectTables(
        inner_select_ast.get(),
        inner_left_table_expr->as<ASTTableExpression>(),
        inner_right_table_expr->as<ASTTableExpression>(),
        inner_table_join->as<ASTTableJoin>());
    if (auto where_expr = children_visited_ast->where())
    {
        inner_select_ast->setExpression(ASTSelectQuery::Expression::WHERE, where_expr->clone());
    }
    auto inner_select_with_union_ast = std::make_shared<ASTSelectWithUnionQuery>();
    inner_select_with_union_ast->list_of_selects = std::make_shared<ASTExpressionList>();
    inner_select_with_union_ast->list_of_selects->children.push_back(inner_select_ast);
    LOG_TRACE(logger, "3) children_visited_ast:{}", queryToString(children_visited_ast_ref));
    String table_alias = tables_with_columns[0].table.alias;
    if (table_alias.empty() && !tables_with_columns[0].table.table.empty())
    {
        table_alias = tables_with_columns[0].table.table;
    }
    ASTBuildUtil::updateSelectLeftTableBySubquery(children_visited_ast, inner_select_with_union_ast.get(), table_alias);
    LOG_TRACE(logger, "4) children_visited_ast:{}", queryToString(children_visited_ast_ref));
    children_visited_ast->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
    LOG_TRACE(logger, "inner_select_ast:{}", queryToString(inner_select_ast));
    LOG_TRACE(logger, "children_visited_ast:{}", queryToString(*children_visited_ast));
    return children_visited_ast_ref;
}

std::shared_ptr<ASTTableExpression> EliminateCompositeColumnNameRewriter::visitJoinedTableExpression(const ASTTableExpression * table_expression)
{
    return std::dynamic_pointer_cast<ASTTableExpression>(table_expression->clone());
}

ASTPtr EliminateCompositeColumnNameRewriter::visit(ASTInsertQuery * ast)
{
    auto res = ast->clone();
    auto * insert_ast = res->as<ASTInsertQuery>();
    insert_ast->select = visitChild(ast->select.get());
    return res;
}

void EliminateCompositeColumnNameRewriter::printColumnAlias()
{
    WriteBufferFromOwnString buf;
    for (const auto & alias : columns_alias)
    {
        buf << "{" << alias.first << ":" << alias.second << "} ";
    }
    LOG_TRACE(logger, "columns_alias={}", buf.str());
}
}
