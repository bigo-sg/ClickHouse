#pragma once

#include <cassert>
#include <list>
#include <memory>
#include <Parsers/IAST_fwd.h>

namespace DB
{
template<class Action>
class ASTDepthFirstVisitor
{
public:
    using Result = typename Action::Result;
    ASTDepthFirstVisitor(Action & action_, const ASTPtr & ast_)
    : action(action_)
    , original_ast(ast_)
    {}

    Result visit()
    {
        visit(original_ast);
        return action.getResult();
    }

private:
    Action & action;
    ASTPtr original_ast;
    void visit(const ASTPtr & ast)
    {
        ASTs children = action.collectChildren(ast);
        action.beforeVisitChildren(ast);
        for (const auto & child : children)
        {
            action.beforeVisitChild(child);
            visit(child);
            action.afterVisitChild(child);
        }
        action.afterVisitChildren(ast);

        return action.visit(ast);
    }
};
struct SimpleVisitFrame
{
    explicit SimpleVisitFrame(const ASTPtr & ast) : original_ast(ast) { }
    ASTPtr original_ast;
    ASTPtr result_ast;
    ASTs children_results;
};
using SimpleVisitFramePtr = std::shared_ptr<SimpleVisitFrame>;

template<typename Frame = SimpleVisitFrame>
struct  SimpleVisitFrameStack
{
    std::list<std::shared_ptr<Frame>> stack;

    inline void pushFrame(const ASTPtr ast)
    {
        stack.emplace_back(std::make_shared<Frame>(ast));
    }

    inline void pushFrame(std::shared_ptr<Frame> frame)
    {
        stack.emplace_back(frame);
    }

    inline std::shared_ptr<Frame> getTopFrame()
    {
        if (stack.empty())
            return nullptr;
        return stack.back();
    }
    inline std::shared_ptr<Frame> getPrevFrame()
    {
        if (stack.size() < 2)
            return nullptr;
        auto iter = stack.rbegin();
        iter++;
        return *iter;
    }
    inline void popFrame()
    {
        stack.pop_back();
    }

    inline size_t size() const
    {
        return stack.size();
    }
};

class EmptyASTDepthFirstVisitAction
{
public:

    virtual ~EmptyASTDepthFirstVisitAction() = default;
    virtual ASTs collectChildren(const ASTPtr & /*current_ast*/) { return {}; }
    virtual void beforeVisitChildren(const ASTPtr & /*current_ast*/) {}
    virtual void beforeVisitChild(const ASTPtr & /*child*/) {}
    virtual void afterVisitChild(const ASTPtr & /*child*/) {}
    virtual void afterVisitChildren(const ASTPtr & /*current_ast*/) {}
};
}
