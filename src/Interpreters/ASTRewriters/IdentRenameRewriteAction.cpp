#include <memory>
#include <Interpreters/ASTRewriters/ASTBuildUtil.h>
#include <Interpreters/ASTRewriters/IdentRenameRewriteAction.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
ASTs IdentifierRenameAction::collectChildren(const ASTPtr & ast)
{
    if (!ast)
        return {};
    if (const auto * function_ast = ast->as<ASTFunction>())
    {
        return function_ast->arguments->children;
    }
    else if (const auto * expr_list_ast = ast->as<ASTExpressionList>())
    {
        return expr_list_ast->children;
    }
    else if (const auto * orderby_ast = ast->as<ASTOrderByElement>())
    {
        ASTs children;
        children.emplace_back(orderby_ast->collation);
        children.emplace_back(orderby_ast->fill_from);
        children.emplace_back(orderby_ast->fill_to);
        children.emplace_back(orderby_ast->fill_step);
        return children;
    }
    else if (const auto * ident_ast = ast->as<ASTIdentifier>())
    {
        return {};
    }
    else if (const auto * asterisk_ast = ast->as<ASTAsterisk>())
    {
        return {};
    }
    else if (const auto * qualified_asterisk_ast = ast->as<ASTQualifiedAsterisk>())
    {
        return {};
    }
    return {};
}
void IdentifierRenameAction::beforeVisitChildren(const ASTPtr & ast)
{
    frames.pushFrame(ast);
}

void IdentifierRenameAction::afterVisitChild(const ASTPtr & /*ast*/)
{
    auto child_result = frames.getTopFrame()->result_ast;
    frames.popFrame();
    assert(frames.size() != 0);
    frames.getTopFrame()->children_results.emplace_back(child_result);
}

void IdentifierRenameAction::visit(const ASTPtr & ast)
{
    assert(!frames.empty());
    if (!ast)
        return;
    if (const auto * ident_ast = ast->as<ASTIdentifier>())
    {
        visit(ident_ast);
    }
    else if (ast->as<ASTFunction>())
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = frame->original_ast->clone();
        auto * result_function_ast = frame->result_ast->as<ASTFunction>();
        result_function_ast->arguments->children = frame->children_results;
    }
    else if (ast->as<ASTExpressionList>())
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = frame->original_ast->clone();
        auto * result_expr_list_ast = frame->result_ast->as<ASTExpressionList>();
        result_expr_list_ast->children = frame->children_results;
    }
    else if (ast->as<ASTOrderByElement>())
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = frame->original_ast->clone();
        auto * result_orderby_ast = frame->result_ast->as<ASTOrderByElement>();
        result_orderby_ast->collation = frame->children_results[0];
        result_orderby_ast->fill_from = frame->children_results[1];
        result_orderby_ast->fill_to = frame->children_results[2];
        result_orderby_ast->fill_step = frame->children_results[3];
    }
    else
    {
        auto frame = frames.getTopFrame();
        if (frame->original_ast)
            frame->result_ast = frame->original_ast->clone();
    }

}

void IdentifierRenameAction::visit(const ASTIdentifier * ident_ast)
{
    auto frame = frames.getTopFrame();
    const auto & name = ident_ast->name();
    auto iter = renamed_idents.find(name);
    if (iter == renamed_idents.end())
        frame->result_ast = ident_ast->clone();
    else
    {
        auto result_ast = std::make_shared<ASTIdentifier>(iter->second);
        result_ast->alias = ident_ast->tryGetAlias();
        frame->result_ast = result_ast;
    }
}

ASTs MakeFunctionColumnAliasAction::collectChildren(const ASTPtr & ast)
{
    if (!ast)
        return {};
    if (const auto * expr_list_ast = ast->as<ASTExpressionList>())
    {
        auto  prev_frame = frames.getTopFrame();
        if (!prev_frame || prev_frame->original_ast->as<ASTSelectQuery>())
        {
            LOG_TRACE(&Poco::Logger::get("MakeFunctionColumnAliasAction"), "is expr list from select. {}", queryToString(ast));
            return expr_list_ast->children;
        }
        return {};
    }
    else if (const auto * table_expr = ast->as<ASTTableExpression>())
    {
        ASTs children;
        if (table_expr->subquery)
        {
            children.emplace_back(table_expr->subquery);
        }
        return children;
    }
    else if (const auto * subquery = ast->as<ASTSubquery>())
    {
        ASTs children;
        children.emplace_back(subquery->children[0]);
        return children;
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        ASTs children;
        children.emplace_back(select_ast->select());

        if (const auto * left_table_expr = getTableExpression(*select_ast, 0))
        {
            children.emplace_back(left_table_expr->clone());
        }

        if (const auto * right_table_expr = getTableExpression(*select_ast, 1))
        {
            children.emplace_back(right_table_expr->clone());
        }
        return children;
    }
    else if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        return select_with_union_ast->list_of_selects->children;
    }
    return {};
}

void MakeFunctionColumnAliasAction::beforeVisitChildren(const ASTPtr & ast)
{
    frames.pushFrame(ast);
}

void MakeFunctionColumnAliasAction::afterVisitChild(const ASTPtr & /*ast*/)
{
    auto child_result = frames.getTopFrame()->result_ast;
    frames.popFrame();
    assert(frames.size() != 0);
    frames.getTopFrame()->children_results.emplace_back(child_result);
}

void MakeFunctionColumnAliasAction::visit(const ASTPtr & ast)
{
    if (const auto * function_ast = ast->as<ASTFunction>())
    {
        visit(function_ast);
    }
    else if (const auto * expr_list_ast = ast->as<ASTExpressionList>())
    {
        visit(expr_list_ast);
    }
    else if (const auto * table_expr = ast->as<ASTTableExpression>())
    {
        visit(table_expr);
    }
    else if (const auto * subquery = ast->as<ASTSubquery>())
    {
        visit(subquery);
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        visit(select_ast);
    }
    else if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        visit(select_with_union_ast);
    }
    else
    {
        frames.getTopFrame()->result_ast = frames.getTopFrame()->original_ast;
    }
}

void MakeFunctionColumnAliasAction::visit(const ASTFunction * function_ast)
{
    auto frame = frames.getTopFrame();
    if (!function_ast->tryGetAlias().empty())
    {
        frame->result_ast = function_ast->clone();
        return;
    }
    auto iter = functions_alias_id->find(function_ast->name);
    size_t id = 0;
    if (iter == functions_alias_id->end())
    {
        (*functions_alias_id)[function_ast->name] = 0;
    }
    else
    {
        id = iter->second++;
    }
    frame->result_ast = function_ast->clone();
    frame->result_ast->setAlias(function_ast->name + "_" + std::to_string(id));
    LOG_TRACE(&Poco::Logger::get("MakeFunctionColumnAliasAction"), "make alias for function: {}", queryToString(frame->result_ast));
}

void MakeFunctionColumnAliasAction::visit(const ASTExpressionList * expr_list_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = expr_list_ast->clone();
    auto * result_expr_ast = frame->result_ast->as<ASTExpressionList>();
    auto prev_frame = frames.getPrevFrame();
    if (!prev_frame || prev_frame->original_ast->as<ASTSelectQuery>())
    {
        LOG_TRACE(&Poco::Logger::get("MakeFunctionColumnAliasAction"), "ASTExpressionList used rewrite children. size: {}. {}", frame->children_results.size(), queryToString(*expr_list_ast));
        result_expr_ast->children = frame->children_results;
    }
}


void MakeFunctionColumnAliasAction::visit(const ASTTableExpression * table_expr_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = table_expr_ast->clone();
    auto * result_table_expr_ast = frame->result_ast->as<ASTTableExpression>();
    if (!frame->children_results.empty())
    {
        result_table_expr_ast->subquery = frame->children_results[0];
    }
}

void MakeFunctionColumnAliasAction::visit(const ASTSubquery * subquery_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = subquery_ast->clone();
    auto * result_subquery_ast = frame->result_ast->as<ASTSubquery>();
    result_subquery_ast->children = frame->children_results;
}

void MakeFunctionColumnAliasAction::visit(const ASTSelectQuery * select_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = select_ast->clone();
    auto * result_select_ast = frame->result_ast->as<ASTSelectQuery>();
    LOG_TRACE(&Poco::Logger::get("MakeFunctionColumnAliasAction"), "ASTSelectQuery: {}", queryToString(*select_ast));
    LOG_TRACE(&Poco::Logger::get("MakeFunctionColumnAliasAction"), "children results size:{}", frame->children_results.size());

    result_select_ast->setExpression(ASTSelectQuery::Expression::SELECT, frame->children_results[0]->clone());
    if (frame->children_results.size() == 2)
    {
        ASTBuildUtil::updateSelectQueryTables(result_select_ast,
            ASTBuildUtil::createTablesInSelectQueryElement(frame->children_results[1]->as<ASTTableExpression>())->as<ASTTablesInSelectQueryElement>());
    }
    else if (frame->children_results.size() == 3)
    {
        ASTBuildUtil::updateSelectQueryTables(
            result_select_ast,
            ASTBuildUtil::createTablesInSelectQueryElement(frame->children_results[1]->as<ASTTableExpression>())
                ->as<ASTTablesInSelectQueryElement>(),
            ASTBuildUtil::createTablesInSelectQueryElement(
                frame->children_results[2]->as<ASTTableExpression>(), select_ast->join()->table_join)
                ->as<ASTTablesInSelectQueryElement>());
    }
}

void MakeFunctionColumnAliasAction::visit(const ASTSelectWithUnionQuery * select_with_union_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = select_with_union_ast->clone();
    auto * result_select_ast = frame->result_ast->as<ASTSelectWithUnionQuery>();
    result_select_ast->list_of_selects->children = frame->children_results;
}

}
