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
class UnresolveIdentifierTableRewriter
{
public:
    explicit UnresolveIdentifierTableRewriter(ContextPtr context_, ASTPtr ast);
    ASTPtr run();
private:
    ContextPtr context;
    ASTPtr from_ast;

    static ASTPtr visitChild(IAST * ast);
    static ASTPtr visit(ASTIdentifier * ast);
    static ASTPtr visit(ASTFunction * ast);
    static ASTPtr visit(ASTExpressionList * ast);
};
}
