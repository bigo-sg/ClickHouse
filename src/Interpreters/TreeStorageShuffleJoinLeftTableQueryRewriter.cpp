#include <memory>
#include <Interpreters/TreeStorageShuffleJoinLeftTableQueryRewriter.h>
#include <Common/ErrorCodes.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/CollectJoinColumnsVisitor.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/queryToString.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}
void TreeStorageShuffleJoinLeftTableQueryRewriteMatcher::visit(ASTPtr & ast_, Data & data_)
{
    auto clone_ast = ast_->clone();// for safty
    
    ASTPtr res;
    if (auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(clone_ast))
    {
        res = visit(select_with_union_query, data_);
    }
    else if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(clone_ast))
    {
        res = visit(select_query, data_);
    }
    else 
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not support query kind: {}", clone_ast->getID());
    }
    if (res)
        data_.rewritten_query = res;
    else
        data_.rewritten_query = ast_;
    LOG_TRACE(&Poco::Logger::get("TreeStorageShuffleJoinLeftTableQueryRewriteMatcher"), "final query:{}", queryToString(data_.rewritten_query));
}

ASTPtr TreeStorageShuffleJoinLeftTableQueryRewriteMatcher::visit(std::shared_ptr<ASTSelectWithUnionQuery> query, Data & data_)
{
    if (query->list_of_selects->children.size() > 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTSelectWithUnionQuery is expected with only one select.");
    }
    auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(query->list_of_selects->children[0]);
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTSelectQuery is expected.");

    query->list_of_selects->children[0] = visit(select_query, data_);
    return query;
}

ASTPtr TreeStorageShuffleJoinLeftTableQueryRewriteMatcher::visit(std::shared_ptr<ASTSelectQuery> query, Data & data_)
{
    const auto * joined_element = query->join();
    if (!joined_element)
    {
        // no need to rewrite
        return query;
    }
    const ASTTableExpression * left_table_expression = getTableExpression(*query, 0);
    if (!left_table_expression->table_function || left_table_expression->table_function->as<ASTFunction>()->name != "hashedChunksStorage")
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function hashedChunksStorage is exprected");
    }

    auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*query), data_.context, true, true);
    std::vector<NamesAndTypesList> tables_columns_from_select = {{},{}};
    CollectJoinColumnsMatcher::Data select_collect_data{tables_with_columns, tables_columns_from_select};
    CollectJoinColumnsVisitor(select_collect_data).visit(query->select());

    auto table_expression = std::dynamic_pointer_cast<ASTTableExpression>(left_table_expression->clone());
    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_in_select = select_query->tables();
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expression);
    table_element->table_expression = table_expression;
    tables_in_select->children.push_back(table_element);

    auto select_expression = std::make_shared<ASTExpressionList>();
    LOG_TRACE(&Poco::Logger::get("TreeStorageShuffleJoinLeftTableQueryRewriteMatcher"), "collect columns:{}", tables_columns_from_select[0].toString());
    for (const auto & name_and_type : tables_columns_from_select[0])
    {
        auto ident = std::make_shared<ASTIdentifier>(name_and_type.name);
        select_expression->children.push_back(ident);
    }
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expression);
    return select_query;
}
}
