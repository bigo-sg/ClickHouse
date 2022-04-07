#include <Interpreters/ASTRewriters/UnresolveIdentifierTableRewriter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <base/logger_useful.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAsterisk.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


UnresolveIdentifierTableRewriter::UnresolveIdentifierTableRewriter(ContextPtr context_, ASTPtr ast)
    : context(context_)
    , from_ast(ast)
{}

ASTPtr UnresolveIdentifierTableRewriter::run()
{
    return visitChild(from_ast.get());
}

ASTPtr UnresolveIdentifierTableRewriter::visitChild(IAST * ast)
{
    ASTPtr res;
    if (auto * ident = ast->as<ASTIdentifier>())
    {
        res = visit(ident);
    }
    else if (auto * function = ast->as<ASTFunction>())
    {
        res = visit(function);
    }
    else if (auto * expression_list = ast->as<ASTExpressionList>())
    {
        res = visit(expression_list);
    }
    else if (auto * literal = ast->as<ASTLiteral>())
    {
        res = ast->clone();
    }
    else if (auto * asterisk = ast->as<ASTAsterisk>())
    {
        res = ast->clone();
    }
    else 
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknow ast type : {}", ast->getID());   
    }
    return res;
}

ASTPtr UnresolveIdentifierTableRewriter::visit(ASTIdentifier * ast)
{
    auto res = std::make_shared<ASTIdentifier>(ast->shortName());
    res->alias = ast->tryGetAlias();
    return res;
}

ASTPtr UnresolveIdentifierTableRewriter::visit(ASTFunction * ast)
{
    auto res = ast->clone();
    auto * func = res->as<ASTFunction>();
    ASTs new_args_children;
    for (auto & child : func->arguments->as<ASTExpressionList>()->children)
    {
        new_args_children.push_back(visitChild(child.get()));
    }
    auto new_args = std::make_shared<ASTExpressionList>();
    new_args->children = new_args_children;
    func->arguments = new_args;
    return res;
}

ASTPtr UnresolveIdentifierTableRewriter::visit(ASTExpressionList * ast)
{
    ASTs children;
    for (auto & child : ast->children)
    {
        children.emplace_back(visitChild(child.get()));
    }
    auto res = std::make_shared<ASTExpressionList>();
    res->children = children;
    return res;
}

}
