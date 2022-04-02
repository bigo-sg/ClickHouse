#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <algorithm>
#include <memory>
#include <sstream>
#include <Poco/StringTokenizer.h>
namespace DB
{

String ColumnWithDetailNameAndType::toString() const
{
    std::ostringstream buf;
    buf << "full_name: " << full_name << ", short_name: " << short_name
        << ", alias_name: " << alias_name;
    buf << ", data_type: " << type->getName();
    return buf.str();
}

void ColumnWithDetailNameAndType::makeAliasByFullName(std::vector<ColumnWithDetailNameAndType> &columns)
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

bool ASTAnalyzeUtil::hasGroupByRecursively(ASTPtr ast)
{
    if (auto * insert_ast = ast->as<ASTInsertQuery>())
    {
        return hasGroupByRecursively(insert_ast->select);
    }
    else if (auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union->list_of_selects->children)
        {
            if (hasGroupByRecursively(child))
                return true;
        }
    }
    else if (auto * select_ast = ast->as<ASTSelectQuery>())
    {
        return select_ast->groupBy() != nullptr;
    }
    return false;
}

bool ASTAnalyzeUtil::hasGroupBy(ASTPtr ast)
{
    if (auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        if (select_with_union_ast->list_of_selects->children.size() > 1)
            return false;
        return hasGroupBy(select_with_union_ast->list_of_selects->children[0]);
    }
    else if (auto * select_ast = ast->as<ASTSelectQuery>())
    {
        return select_ast->groupBy() != nullptr;
    }
    return false;
}

bool ASTAnalyzeUtil::hasAggregationColumn(ASTPtr ast)
{
    if (auto * select_ast = ast->as<ASTSelectQuery>())
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

void ASTBuildUtil::updateSelectLeftTableBySubquery(ASTSelectQuery * select, ASTSelectWithUnionQuery * subquery, const String & alias)
{
    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->subquery = std::make_shared<ASTSubquery>();
    table_expr->subquery->children.push_back(subquery->clone());
    if (!alias.empty())
    {
        table_expr->subquery->as<ASTSubquery>()->alias = alias;
    }

    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expr);
    table_element->table_expression = table_expr;

    select->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select->tables();
    tables->children.push_back(table_element);
}

void ASTBuildUtil::updateJoinedSelectTables(ASTSelectQuery * select, ASTTableExpression * left_table, ASTTableExpression * right_table, ASTTableJoin * join)
{
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select->tables();
    auto left_table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    left_table_element->table_expression = left_table->clone();
    left_table_element->children.push_back(left_table_element->table_expression);
    tables->children.push_back(left_table_element);

    auto right_table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    right_table_element->table_join = join->clone();
    right_table_element->table_expression = right_table->clone();
    right_table_element->children.push_back(right_table_element->table_expression);
    tables->children.push_back(right_table_element);
}
}
