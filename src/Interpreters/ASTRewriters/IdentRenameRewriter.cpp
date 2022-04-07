#include <memory>
#include <Interpreters/ASTRewriters/IdentRenameRewriter.h>
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
IdentRenameRewriter::IdentRenameRewriter(
    ContextPtr context_,
    ASTPtr ast_,
    std::map<String, String> & alias_,
    std::map<String, size_t> & function_alias_id_,
    bool rename_function_)
    : context(context_), from_ast(ast_), alias(alias_), function_alias_id(function_alias_id_), rename_function(rename_function_)
{}

ASTPtr IdentRenameRewriter::run()
{
    auto res = visitChild(from_ast.get());
    LOG_TRACE(logger, "result query: {}", queryToString(res));
    return res;

}

ASTPtr IdentRenameRewriter::visitChild(IAST * ast)
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

ASTPtr IdentRenameRewriter::visit(ASTIdentifier * ast)
{
    const auto & name = ast->name();
    auto iter = alias.find(name);
    if (iter == alias.end())
        return ast->clone();
    auto res = std::make_shared<ASTIdentifier>(iter->second);
    res->alias = ast->tryGetAlias();
    LOG_TRACE(logger, "rename ident from {}({}) to {}({})", ast->name(), ast->tryGetAlias(), res->name(), res->tryGetAlias());
    return res;
}

ASTPtr IdentRenameRewriter::visit(ASTFunction * ast)
{
    auto res = ast->clone();
    auto * func = res->as<ASTFunction>();
    if (ast->tryGetAlias().empty() && rename_function)
        func->alias = getFunctionAlias(func->name);
    LOG_TRACE(logger, "rename function before:{}", queryToString(res));
    ASTs new_args_children;
    for (auto & child : func->arguments->as<ASTExpressionList>()->children)
    {
        new_args_children.push_back(visitChild(child.get()));
    }
    auto new_args = std::make_shared<ASTExpressionList>();
    new_args->children = new_args_children;
    func->arguments = new_args;
    LOG_TRACE(logger, "rename function after:{}", queryToString(res));
    return res;
}

ASTPtr IdentRenameRewriter::visit(ASTExpressionList * ast)
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

String IdentRenameRewriter::getFunctionAlias(const String & function_name)
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
}
