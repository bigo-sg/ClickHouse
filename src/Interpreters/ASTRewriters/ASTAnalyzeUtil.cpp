#include <algorithm>
#include <memory>
#include <sstream>
#include <Core/NamesAndTypes.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/StringTokenizer.h>
namespace DB
{

String ColumnWithDetailNameAndType::toString() const
{
    WriteBufferFromOwnString buf;
    buf << "full_name: " << full_name << ", short_name: " << short_name
        << ", alias_name: " << alias_name;
    buf << ", data_type: " << type->getName();
    return buf.str();
}

NamesAndTypesList ColumnWithDetailNameAndType::toNamesAndTypesList(const std::vector<ColumnWithDetailNameAndType> & columns)
{
    std::list<NameAndTypePair> names_and_types;
    for (const auto & col : columns)
    {
        names_and_types.emplace_back(NameAndTypePair(col.short_name, col.type));
    }
    NamesAndTypesList res(names_and_types.begin(), names_and_types.end());
    return res;
}

void ColumnWithDetailNameAndType::makeAliasByFullName(std::vector<ColumnWithDetailNameAndType> & columns)
{
    for (auto & column : columns)
    {
        if (column.full_name != column.short_name && column.alias_name.empty())
        {
            column.alias_name = column.full_name;
            std::replace(column.alias_name.begin(), column.alias_name.end(), '.', '_');
        }
    }
}

std::vector<String> ColumnWithDetailNameAndType::splitedFullName() const
{
    Poco::StringTokenizer splitter(full_name, ".");
    std::vector<String> res;
    for (const auto & token : splitter)
    {
        res.push_back(token);
    }
    return res;
}

bool ASTAnalyzeUtil::hasGroupByRecursively(const ASTPtr & ast)
{
    return hasGroupByRecursively(ast.get());
}
bool ASTAnalyzeUtil::hasGroupByRecursively(const IAST * ast)
{
    if (!ast)
        return false;
    if (const auto * insert_ast = ast->as<ASTInsertQuery>())
    {
        return hasGroupByRecursively(insert_ast->select);
    }
    else if (const auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union->list_of_selects->children)
        {
            if (hasGroupByRecursively(child))
                return true;
        }
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        if (select_ast->groupBy() != nullptr)
            return true;
        return hasGroupByRecursively(select_ast->groupBy().get());
    }
    else if (const auto * tables_ast = ast->as<ASTTablesInSelectQuery>())
    {
        for (const auto & child : tables_ast->children)
        {
            if (hasGroupByRecursively(child.get()))
                return true;
        }
    }
    else if (const auto * table_element = ast->as<ASTTablesInSelectQueryElement>())
    {
        const auto * table_expr = table_element->table_expression->as<ASTTableExpression>();
        return hasGroupByRecursively(table_expr->subquery.get());
    }
    else if (const auto * subquery = ast->as<ASTSubquery>())
    {
        for (const auto & child : subquery->children)
        {
            if (hasGroupByRecursively(child.get()))
                return true;
        }
    }
    return false;
}


bool ASTAnalyzeUtil::hasGroupBy(const ASTPtr & ast)
{
    return hasGroupBy(ast.get());
}

bool ASTAnalyzeUtil::hasGroupBy(const IAST * ast)
{
    if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        if (select_with_union_ast->list_of_selects->children.size() > 1)
            return false;
        return hasGroupBy(select_with_union_ast->list_of_selects->children[0]);
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        return select_ast->groupBy() != nullptr;
    }
    return false;
}

bool ASTAnalyzeUtil::hasAggregationColumn(const ASTPtr & ast)
{
    return hasAggregationColumn(ast.get());
}
bool ASTAnalyzeUtil::hasAggregationColumn(const IAST * ast)
{
    if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        const auto * select_list = select_ast->select()->as<ASTExpressionList>();
        for (const auto & child : select_list->children)
        {
            if (const auto * function = child->as<ASTFunction>())
            {
                if (function->name == "count" || function->name == "avg" || function->name == "sum")
                {
                    return true;
                }
            }
        }
    }
    return false;
}

bool ASTAnalyzeUtil::hasAggregationColumnRecursively(const ASTPtr & ast)
{
    return hasAggregationColumnRecursively(ast.get());
}

bool ASTAnalyzeUtil::hasAggregationColumnRecursively(const IAST * ast)
{
    if (!ast)
        return false;
    if (const auto * insert_ast = ast->as<ASTInsertQuery>())
    {
        return hasAggregationColumnRecursively(insert_ast->select.get());
    }
    else if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        for (const auto & child : select_with_union_ast->list_of_selects->children)
        {
            if (hasAggregationColumnRecursively(child.get()))
                return true;
        }
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        if (hasAggregationColumn(select_ast))
            return true;
        return hasAggregationColumnRecursively(select_ast->tables().get());
    }
    else if (const auto * tables_ast = ast->as<ASTTablesInSelectQuery>())
    {
        for (const auto & child : tables_ast->children)
        {
            if (hasAggregationColumnRecursively(child.get()))
                return true;
        }
    }
    else if (const auto * table_element = ast->as<ASTTablesInSelectQueryElement>())
    {
        const auto * table_expr = table_element->table_expression->as<ASTTableExpression>();
        return hasAggregationColumnRecursively(table_expr->subquery.get());
    }
    else if (const auto * subquery = ast->as<ASTSubquery>())
    {
        for (const auto & child : subquery->children)
        {
            if (hasAggregationColumnRecursively(child.get()))
                return true;
        }
    }
    return false;
}

String ASTAnalyzeUtil::tryGetTableExpressionAlias(const ASTTableExpression * table_expr)
{
    String res;
    if (table_expr->table_function)
    {
        res = table_expr->table_function->as<ASTFunction>()->tryGetAlias();
    }
    else if (table_expr->subquery)
    {
        res = table_expr->subquery->as<ASTSubquery>()->tryGetAlias();
    }
    else if (table_expr->database_and_table_name)
    {
        if (const auto * with_alias_ast = table_expr->database_and_table_name->as<ASTTableIdentifier>())
        {
            res = with_alias_ast->tryGetAlias();
            if (res.empty())
            {
                res = with_alias_ast->shortName();
            }
        }
    }
    return res;
}

}
