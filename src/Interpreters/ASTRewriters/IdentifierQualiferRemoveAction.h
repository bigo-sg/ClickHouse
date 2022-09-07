#pragma once
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Parsers/IAST_fwd.h>
namespace DB
{
class IdentifiterQualiferRemoveAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = ASTPtr;
    ~IdentifiterQualiferRemoveAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override;
    void beforeVisitChildren(const ASTPtr & ast) override;
    void afterVisitChild(const ASTPtr & ast) override;
    void visit(const ASTPtr & ast);

    Result getResult()
    {
        return frames.getTopFrame()->result_ast;
    }
private:
    SimpleVisitFrameStack<> frames;
};
}
