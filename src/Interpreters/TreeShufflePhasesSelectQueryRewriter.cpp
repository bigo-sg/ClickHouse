#include <Interpreters/TreeShufflePhasesSelectQueryRewriter.h>
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

namespace DB
{
void TreeShufflePhasesSelectQueryRewriterMatcher::visit(ASTPtr & ast_, Data & data_)
{
    ASTPtr top_ast;
    ASTPtr from_ast = ast_->clone();
    UInt16 frame_index;
    if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(ast_))
    {
        top_ast = visit(select_query, data_, frame_index);
    }
    else if (auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(ast_))
    {
        top_ast = visit(select_with_union_query, data_, frame_index);
    }

    if (!top_ast)
    {
        data_.rewritten_query = from_ast;
    }
    else
    {
        //
        auto shuffle_phases_ast = std::make_shared<ASTShufflePhasesSelectQuery>();
        shuffle_phases_ast->final_query = top_ast;
        for (auto & phase : data_.shuffle_phases)
        {
            shuffle_phases_ast->shuffle_phases.emplace_back(std::move(phase.second));
        }
        data_.rewritten_query = shuffle_phases_ast;
    }

    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriteMatcher"), "stage query:{}", queryToString(data_.rewritten_query));
}

ASTPtr TreeShufflePhasesSelectQueryRewriterMatcher::visit(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, UInt16 & frame_index)
{
    const auto * joined_element = query_->join();
    if (joined_element)
    {
        auto new_query =  visitSelectWithJoin(query_, data_, frame_index);
        return new_query;
    }
    auto left_table_expression = extractTableExpression(*query_, 0);
    if (left_table_expression && left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        auto subquery = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(left_table_expression->clone());
        auto new_subquery = visit(subquery, data_, frame_index);
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

        return new_select_query;   
    }
    return nullptr;
}

ASTPtr TreeShufflePhasesSelectQueryRewriterMatcher::visit(std::shared_ptr<ASTSelectWithUnionQuery> & query_, Data & data_, UInt16 & frame_index)
{
    ASTs prev_children;
    query_->list_of_selects->children.swap(prev_children);
    auto & new_children = query_->list_of_selects->children;
    for (auto & child : prev_children)
    {
        auto new_child = visitChild(child, data_, frame_index);
        if (!new_child)
            return nullptr; 
        new_children.emplace_back(new_child);
    }
    return query_;
}

ASTPtr TreeShufflePhasesSelectQueryRewriterMatcher::visitChild(ASTPtr & query_, Data & data_, UInt16 & frame_index)
{
    ASTPtr new_query;
    if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(query_))
    {
        new_query = visit(select_query, data_, frame_index);
    }
    else if (auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(query_))
    {
        new_query = visit(select_with_union_query, data_, frame_index);   
    }
    else
    {
        new_query = query_;
    }
    if (!new_query)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Rewrite ast failed. {}", queryToString(query_));
        //LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriteMatcher"), "Rewrite ast failed. {}", queryToString(query_));
    }
    return new_query;
}

ASTPtr TreeShufflePhasesSelectQueryRewriterMatcher::visitSelectWithJoin(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, UInt16 & frame_index)
{
    TreeDistributedShuffleJoinRewriter rewriter(query_, data_.context, data_.id_count);
    if (!rewriter.prepare())
        return nullptr;
    
    UInt16 left_frame_index = 0, right_frame_index = 0;
    auto new_left_table_expression = visitJoinSelectTableExpression(getTableExpression(*query_, 0), data_, left_frame_index);
    if (!new_left_table_expression)
        return nullptr;
    auto new_right_table_expression = visitJoinSelectTableExpression(getTableExpression(*query_, 1), data_, right_frame_index);
    if (!new_right_table_expression)
        return nullptr;

    auto left_table_id = rewriter.getHashTableId("left");
    auto right_table_id = rewriter.getHashTableId("right");
    
    auto left_insert_query = rewriter.createSubJoinTable(left_table_id, new_left_table_expression, 0);
    auto right_insert_query = rewriter.createSubJoinTable(right_table_id, new_right_table_expression, 1);
    auto new_query = rewriter.createNewJoinSelectQuery(left_table_id, right_table_id);
    
    auto & left_phase = data_.shuffle_phases[left_frame_index];
    left_phase.emplace_back(left_insert_query);
    auto & right_phase = data_.shuffle_phases[right_frame_index];
    right_phase.emplace_back(right_insert_query);
    frame_index = std::max(left_frame_index, right_frame_index) + 1;

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
std::shared_ptr<ASTTableExpression> TreeShufflePhasesSelectQueryRewriterMatcher::visitJoinSelectTableExpression(const ASTTableExpression * table_expression, Data & data, UInt16 & frame_index)
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
        auto new_left_subquery = visit(slect_query_with_union, data, frame_index);
        new_table_expression = std::make_shared<ASTTableExpression>();
        auto subquery = std::make_shared<ASTSubquery>();
        subquery->children.push_back(new_left_subquery);
        subquery->setAlias(tryGetTableExpressionAlias(table_expression));   
        new_table_expression->subquery = subquery;
    }

    return new_table_expression;
}
}
