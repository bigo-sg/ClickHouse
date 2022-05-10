#pragma once
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/WindowDescription.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/Impl/ConstantExpressionTemplate.h>
#include <Poco/Logger.h>
namespace DB
{

class IdentifierRenameAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = ASTPtr;
    struct VisitFrame
    {
        explicit VisitFrame(const ASTPtr & ast) : original_ast(ast) {}
        const ASTPtr & original_ast;
        ASTPtr result_ast;
        ASTs children_results;
    };

    explicit IdentifierRenameAction(ContextPtr context_, const std::map<String, String> & renamed_idents_)
    : context(context_)
    , renamed_idents(renamed_idents_)
    {}

    ~IdentifierRenameAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override;
    void beforeVisitChildren(const ASTPtr & ast) override;
    void afterVisitChild(const ASTPtr & ast) override;
    void visit(const ASTPtr & ast);

    ASTPtr getResult()
    {
        assert(frames.size() == 1);
        return frames.getTopFrame()->result_ast;
    }

private:
    ContextPtr context;
    std::map<String, String> renamed_idents;
    SimpleVisitFrameStack<> frames;

    void visit(const ASTIdentifier * ident_ast);
};

class MakeFunctionColumnAliasAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = ASTPtr;
    explicit MakeFunctionColumnAliasAction(std::map<String, size_t> * functions_alias_id_ = nullptr) : functions_alias_id(functions_alias_id_)
    {
        if (!functions_alias_id)
            functions_alias_id = &local_functions_alias_id;
    }
    ~MakeFunctionColumnAliasAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override;
    void beforeVisitChildren(const ASTPtr & ast) override;
    void afterVisitChild(const ASTPtr & ast) override;
    void visit(const ASTPtr & ast);

    ASTPtr getResult()
    {
        assert(frames.size() == 1);
        return frames.getTopFrame()->result_ast;
    }
private:
    std::map<String, size_t> * functions_alias_id;
    std::map<String, size_t> local_functions_alias_id;
    SimpleVisitFrameStack<> frames;

    void visit(const ASTFunction * function_ast);
    void visit(const ASTExpressionList * expr_list_ast);
    void visit(const ASTTableExpression * table_expr_ast);
    void visit(const ASTSubquery * subquery_ast);
    void visit(const ASTSelectQuery * select_ast);
    void visit(const ASTSelectWithUnionQuery * select_with_union_ast);
};
}
