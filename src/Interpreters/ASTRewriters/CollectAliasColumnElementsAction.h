#pragma once
#include <map>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
namespace DB
{
class CollectAliasColumnElementAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = std::map<std::string, ASTPtr>;
    ~CollectAliasColumnElementAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override
    {
        if (!ast)
            return {};
        if (const auto * expr_list_ast = ast->as<ASTExpressionList>())
        {
            return expr_list_ast->children;
        }
        else if (const auto * select_ast = ast->as<ASTSelectQuery>())
        {
            return ASTs{select_ast->select()};
        }
        else if (const auto * union_select_ast = ast->as<ASTSelectWithUnionQuery>())
        {
            return union_select_ast->list_of_selects->children;
        }
        return {};
    }

    void visit(const ASTPtr & ast)
    {
        if (const auto * ident_ast = ast->as<ASTIdentifier>())
        {
            auto alias = ident_ast->tryGetAlias();
            if (!alias.empty())
            {
                result[alias] = ast->clone();
            }

        }
        else if (const auto * function_ast = ast->as<ASTFunction>())
        {
            auto alias = function_ast->tryGetAlias();
            if (!alias.empty())
            {
                result[alias] = ast->clone();
            }
        }
    }

    Result getResult() { return result; }

private:
    Result result;

};
}
