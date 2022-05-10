#pragma once

#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTStageQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
namespace DB
{
/**
 * @brief For constucting a insert query to signal a shuffle block table has finished insertion. Always follow a insert query
 * that inserts into a shuffle block table.
 *
 */

class StageQueryShuffleFinishEventRewriteAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = ASTPtr;
    StageQueryShuffleFinishEventRewriteAction() = default;
    ~StageQueryShuffleFinishEventRewriteAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override;
    void beforeVisitChildren(const ASTPtr & ast) override;
    void afterVisitChild(const ASTPtr & ast) override;
    void visit(const ASTPtr & ast);

    Result getResult()
    {
        auto frame = frames.getTopFrame();
        return frame->result_ast;
    }
private:
    SimpleVisitFrameStack<> frames;

    void visit(const ASTStageQuery * stage_ast);
    void visit(const ASTInsertQuery * insert_ast);
};
}
