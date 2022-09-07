#include <memory>
#include <Interpreters/ASTRewriters/IdentifierQualiferRemoveAction.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryToString.h>
#include "Parsers/ASTExpressionList.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
ASTs IdentifiterQualiferRemoveAction::collectChildren(const ASTPtr & ast)
{
    if (!ast)
        return {};
    ASTs children;
    if (const auto * function_ast = ast->as<ASTFunction>())
    {
        children = function_ast->arguments->children;
    }
    else if (const auto * expr_list_ast = ast->as<ASTExpressionList>())
    {
        children = expr_list_ast->children;
    }
    return children;
}

void IdentifiterQualiferRemoveAction::visit(const ASTPtr & ast)
{
    if (!ast)
        return;
    if (const auto * function_ast = ast->as<ASTFunction>())
    {
        auto frame = frames.getTopFrame();
        auto result_function_ast = std::make_shared<ASTFunction>();
        result_function_ast->name = function_ast->name;
        result_function_ast->arguments = std::make_shared<ASTExpressionList>();
        result_function_ast->arguments->children = frame->children_results;
        frame->result_ast = result_function_ast;
        result_function_ast->alias = function_ast->tryGetAlias();
    }
    else if (const auto * literal_ast = ast->as<ASTLiteral>())
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = frame->original_ast->clone();
    }
    else if (const auto * ident_ast = ast->as<ASTIdentifier>())
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = std::make_shared<ASTIdentifier>(ident_ast->shortName());
        auto * result_ast = frame->result_ast->as<ASTIdentifier>();
        result_ast->alias = ident_ast->tryGetAlias();
    }
    else if (const auto * expr_list_ast = ast->as<ASTExpressionList>())
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = std::make_shared<ASTExpressionList>();
        auto * result_ast = frame->result_ast->as<ASTExpressionList>();
        result_ast->children = frame->children_results;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid ast({}): {}", ast->getID(), queryToString(ast));
    }
}

void IdentifiterQualiferRemoveAction::beforeVisitChildren(const ASTPtr & ast)
{
    frames.pushFrame(ast);
}

void IdentifiterQualiferRemoveAction::afterVisitChild(const ASTPtr & /*ast*/)
{
    auto result_ast = frames.getTopFrame()->result_ast;
    frames.popFrame();
    frames.getTopFrame()->children_results.emplace_back(result_ast);
}
}
