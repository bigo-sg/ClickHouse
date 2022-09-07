#include <Core/Field.h>
#include <Interpreters/ASTRewriters/ASTBuildUtil.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String ASTBuildUtil::getTableExpressionAlias(const ASTTableExpression * ast)
{
    String res;
    if (ast->table_function)
    {
        res = ast->table_function->as<ASTFunction>()->tryGetAlias();
    }
    else if (ast->subquery)
    {
        res = ast->subquery->as<ASTSubquery>()->tryGetAlias();
    }
    else if (ast->database_and_table_name)
    {
        if (const auto * with_alias_ast = ast->database_and_table_name->as<ASTTableIdentifier>())
            res = with_alias_ast->tryGetAlias();
    }
    return res;
}

std::shared_ptr<ASTExpressionList> ASTBuildUtil::toShortNameExpressionList(const ColumnWithDetailNameAndTypes & columns)
{
    auto expression_list = std::make_shared<ASTExpressionList>();
    for (const auto & col : columns)
    {
        auto ident = std::make_shared<ASTIdentifier>(col.short_name);
        expression_list->children.push_back(ident);
    }
    return expression_list;
}
String ASTBuildUtil::toTableStructureDescription(const ColumnWithDetailNameAndTypes & columns)
{
    WriteBufferFromOwnString buf;
    int i = 0;
    for (const auto & col : columns)
    {
        if (i)
        {
            buf << ",";
        }
        buf << col.short_name << " " << col.type->getName();
        i++;
    }
    return buf.str();
}


ASTPtr ASTBuildUtil::createShuffleTableFunction(
    const String & function_name,
    const String & cluster_name,
    const String & session_id,
    const String & table_id,
    const NamesAndTypesList & columns,
    const ASTPtr & hash_expression_list,
    const String & alias)
{
    auto table_func = std::make_shared<ASTFunction>();
    table_func->name = function_name;
    table_func->arguments = std::make_shared<ASTExpressionList>();
    table_func->children.push_back(table_func->arguments);

    Field cluster_name_field(cluster_name);
    table_func->arguments->children.push_back(std::make_shared<ASTLiteral>(cluster_name_field));

    table_func->arguments->children.push_back(std::make_shared<ASTLiteral>(Field(session_id)));
    table_func->arguments->children.push_back(std::make_shared<ASTLiteral>(Field(table_id)));

    WriteBufferFromOwnString struct_buf;
    int i = 0;
    for (const auto & name_and_type : columns)
    {
        if (i)
            struct_buf << ",";
        struct_buf << name_and_type.name << " " << name_and_type.type->getName();
        i++;
    }
    auto hash_table_structure = std::make_shared<ASTLiteral>(struct_buf.str());
    table_func->arguments->children.push_back(hash_table_structure);

    if (hash_expression_list)
    {
        auto hash_table_key = std::make_shared<ASTLiteral>(queryToString(hash_expression_list));
        table_func->arguments->children.push_back(hash_table_key);
    }
    if (!alias.empty())
        table_func->setAlias(alias);
    return table_func;
}

ASTPtr ASTBuildUtil::createTableFunctionInsertSelectQuery(ASTPtr table_function, ASTPtr select_query)
{
    if (!table_function->as<ASTFunction>() || !select_query->as<ASTSelectWithUnionQuery>())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalide ast type.");
    }

    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->table_function = table_function;
    insert_query->select = select_query;
    return insert_query;
}

ASTPtr ASTBuildUtil::wrapSelectQuery(const ASTSelectQuery * select_query)
{
    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(select_query->clone());
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->children.push_back(list_of_selects);
    select_with_union_query->list_of_selects = list_of_selects;
    return select_with_union_query;
}

ASTPtr ASTBuildUtil::createSelectExpression(const NamesAndTypesList & names_and_types)
{
    auto select_expression = std::make_shared<ASTExpressionList>();
    for (const auto & name_and_type : names_and_types)
    {
        auto ident = std::make_shared<ASTIdentifier>(name_and_type.name);
        select_expression->children.push_back(ident);
    }
    return select_expression;
}

void ASTBuildUtil::updateSelectQueryTables(ASTSelectQuery * select_query, const ASTTableExpression * table_expr_)
{
    auto table_expr = table_expr_->clone();
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_in_select = select_query->tables();
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expr);
    table_element->table_expression = table_expr;
    tables_in_select->children.push_back(table_element);
}

void ASTBuildUtil::updateSelectQueryTables(ASTSelectQuery * select_query, const ASTTablesInSelectQueryElement * table_element_)
{
    auto table_element = table_element_->clone();
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select_query->tables();
    tables->children.push_back(table_element);
}


void ASTBuildUtil::updateSelectQueryTables(
    ASTSelectQuery * select_query,
    const ASTTablesInSelectQueryElement * left_table_element_,
    const ASTTablesInSelectQueryElement * right_table_element_)
{
    auto left_table_element = left_table_element_->clone();
    auto right_table_element = right_table_element_->clone();
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select_query->tables();
    tables->children.push_back(left_table_element);
    tables->children.push_back(right_table_element);

}

ASTPtr ASTBuildUtil::createTablesInSelectQueryElement(const ASTTableExpression * table_expr_, ASTPtr table_join_)
{
    auto table_expr = table_expr_->clone();
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expr);
    table_element->table_expression = table_expr;
    if (table_join_)
        table_element->table_join = table_join_->clone();
    return table_element;
}
ASTPtr ASTBuildUtil::createTablesInSelectQueryElement(const ASTFunction * func_, ASTPtr table_join_)
{
    auto func = func_->clone();
    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->table_function = func;

    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expr);
    table_element->table_expression = table_expr;
    if (table_join_)
        table_element->table_join = table_join_->clone();
    return table_element;
}

ASTPtr ASTBuildUtil::createTablesInSelectQueryElement(const ASTSelectWithUnionQuery * select_query, const String & alias)
{
    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->subquery = std::make_shared<ASTSubquery>();
    table_expr->subquery->children.push_back(select_query->clone());
    if (!alias.empty())
    {
        table_expr->subquery->as<ASTSubquery>()->alias = alias;
    }

    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expr);
    table_element->table_expression = table_expr;

    return table_element;
}

}
