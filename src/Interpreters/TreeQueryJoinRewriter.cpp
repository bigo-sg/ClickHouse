#include <memory>
#include <Interpreters/TreeQueryJoinRewriter.h>
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
#include <Interpreters/TreeDistributedShuffleJoinRewriter.h>
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
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSubquery.h>
#include <boost/math/tools/roots.hpp>

namespace DB
{
void TreeQueryJoinRewriterMatcher::visit(ASTPtr & ast_, Data & data_)
{
    ASTPtr from_ast = ast_->clone();
    ASTPtr result;
    if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(ast_))
    {
        std::vector<ASTPtr> children;
        result = visit(select_query, data_, 0, children);
    }
    else if (auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(ast_))
    {
        std::vector<ASTPtr> children;
        result = visit(select_with_union_query, data_, 0, children);
    }
    else
    {
        LOG_INFO(&Poco::Logger::get("TreeQueryJoinRewriterMatcher"), "Not support query type: {}", ast_->getID());
    }
    if (!result)
    {
        data_.rewritten_query = ast_;
    }
    else {
        data_.rewritten_query = result;
    }
}

ASTPtr TreeQueryJoinRewriterMatcher::visit(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, size_t visit_depth_, ASTs & children)
{
    const auto * joined_element = query_->join();
    if (joined_element)
    {
        auto new_query = visitSelectWithJoin(query_, data_, visit_depth_, children);
        return new_query;
    }

    auto left_table_expression = extractTableExpression(*query_, 0);
    if (left_table_expression && left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        auto subquery = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(left_table_expression->clone());
        auto new_subquery = visit(subquery, data_, visit_depth_ + 1, children);
        if (!new_subquery)
            return nullptr;

        auto table_expr = std::make_shared<ASTTableExpression>();
        table_expr->subquery = std::make_shared<ASTSubquery>();
        table_expr->subquery->children.push_back(new_subquery);

        auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
        table_element->children.push_back(table_expr);
        table_element->table_expression = table_expr;

        auto new_select_query = std::dynamic_pointer_cast<ASTSelectQuery>(query_->clone());
        new_select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto tables = new_select_query->tables();
        tables->children.push_back(table_element);

        if (visit_depth_ || children.empty())
            return new_select_query;
        
        // Only when we have join actions on subquery, we rebuild a ASTTreeQuery
        return makeTreeQuery(new_select_query, children);
    }
    //LOG_INFO(&Poco::Logger::get("TreeQueryJoinRewriterMatcher"), "Not support query type: {}", left_table_expression->getID());
    return query_;
}

ASTPtr TreeQueryJoinRewriterMatcher::visit(std::shared_ptr<ASTSelectWithUnionQuery> & query_, Data & data_, size_t visit_depth_, ASTs & children)
{
    ASTs prev_children;
    query_->list_of_selects->children.swap(prev_children);
    auto & new_children = query_->list_of_selects->children;
    for (auto & child : prev_children)
    {
        auto new_child = visitChild(child, data_, visit_depth_ + 1, children);
        if (!new_child)
            return nullptr; 
        new_children.emplace_back(new_child);
    }
    if (visit_depth_ || children.empty())
        return query_;

    return makeTreeQuery(query_, children);
}

ASTPtr TreeQueryJoinRewriterMatcher::visitChild(ASTPtr & query_, Data & data_, size_t visit_depth_, ASTs & children)
{
    ASTPtr new_query;
    if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(query_))
    {
        new_query = visit(select_query, data_, visit_depth_, children);
    }
    else if (auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(query_))
    {
        new_query = visit(select_with_union_query, data_, visit_depth_, children);   
    }
    else
    {
        new_query = query_;
    }
    return new_query;
}

ASTPtr TreeQueryJoinRewriterMatcher::visitSelectWithJoin(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, size_t visit_depth_, ASTs & children)
{
    TreeDistributedShuffleJoinRewriter rewriter(query_, data_.context, data_.id_count);
    if (!rewriter.prepare())
    {
        LOG_TRACE(&Poco::Logger::get("TreeQueryJoinRewriterMatcher"), "TreeDistributedShuffleJoinRewriter::prepare failed");
        return query_;
    }

    ASTs left_children, right_children;
    auto new_left_table_expression = visitJoinSelectTableExpression(getTableExpression(*query_, 0), data_, left_children);
    if (!new_left_table_expression)
        return nullptr;
    auto new_right_table_expression = visitJoinSelectTableExpression(getTableExpression(*query_, 1), data_, right_children);
    if (!new_right_table_expression)
        return nullptr;

    auto left_table_id = rewriter.getHashTableId("left");
    auto right_table_id = rewriter.getHashTableId("right");
    ASTPtr left_insert_query = rewriter.createSubJoinTable(left_table_id, new_left_table_expression, 0);
    ASTPtr right_insert_query = rewriter.createSubJoinTable(right_table_id, new_right_table_expression, 1);
    if (!left_children.empty())
        left_insert_query = makeTreeQuery(left_insert_query, left_children);
    if (!right_children.empty())
        right_insert_query = makeTreeQuery(right_insert_query, right_children);

    auto new_query = rewriter.createNewJoinSelectQuery(left_table_id, right_table_id);

    children.emplace_back(left_insert_query);
    children.emplace_back(right_insert_query);
    if (visit_depth_)
    {
        return new_query;
    }
    
    new_query = makeTreeQuery(new_query, children);
    children.clear();
    return new_query;   
}
static String tryGetTableExpressionAlias(const ASTTableExpression * table_expr)
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
    else if (table_expr->database_and_table_name){
        if (const auto * with_alias_ast = table_expr->database_and_table_name->as<ASTTableIdentifier>())
            res = with_alias_ast->tryGetAlias();
    }
    return res;
}

std::shared_ptr<ASTTableExpression> TreeQueryJoinRewriterMatcher::visitJoinSelectTableExpression(const ASTTableExpression * table_expression, Data & data, ASTs & children)
{
    std::shared_ptr<ASTTableExpression> new_table_expression;
    if (table_expression->database_and_table_name || table_expression->table_function)
    {
        new_table_expression = std::dynamic_pointer_cast<ASTTableExpression>(table_expression->clone());
    }
    else if (table_expression->subquery)
    {
        data.id_count++;
        auto slect_query_with_union = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(table_expression->subquery->children[0]->clone());
        auto new_left_subquery = visit(slect_query_with_union, data, 1, children);
        new_table_expression = std::make_shared<ASTTableExpression>();
        auto subquery = std::make_shared<ASTSubquery>();
        subquery->children.push_back(new_left_subquery);
        subquery->setAlias(tryGetTableExpressionAlias(table_expression));   
        new_table_expression->subquery = subquery;
    }

    return new_table_expression;
}

std::shared_ptr<ASTTreeQuery> TreeQueryJoinRewriterMatcher::makeTreeQuery(ASTPtr output_query, const ASTs & input_queries)
{
    auto tree_query = std::make_shared<ASTTreeQuery>();
    tree_query->output_ast = output_query;
    tree_query->input_asts = input_queries;
    tree_query->children.insert(tree_query->children.end(), input_queries.begin(), input_queries.end());
    tree_query->children.push_back(output_query);
    LOG_TRACE(&Poco::Logger::get("TreeQueryJoinRewriterMatcher"), "Build a tree query: {}", queryToString(tree_query));
    return tree_query;
}

}
