#pragma once
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Formats/Impl/ConstantExpressionTemplate.h>
namespace DB
{
class IdentRenameRewriter
{
public:
    IdentRenameRewriter(
        ContextPtr context_,
        ASTPtr ast_,
        std::map<String, String> & alias_,
        std::map<String, size_t> & function_alias_id_,
        bool rename_function_ = false);
    ASTPtr run();
private:
    ContextPtr context;
    ASTPtr from_ast;
    std::map<String, String> & alias;
    std::map<String, size_t> & function_alias_id;
    bool rename_function;
    Poco::Logger * logger = &Poco::Logger::get("IdentRenameRewriter");

    ASTPtr visitChild(IAST * ast);
    ASTPtr visit(ASTIdentifier * ast);
    ASTPtr visit(ASTFunction * ast);
    ASTPtr visit(ASTExpressionList * ast);

    String getFunctionAlias(const String & function_name);
};
}
