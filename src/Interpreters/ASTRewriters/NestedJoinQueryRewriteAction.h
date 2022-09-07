#pragma once
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
namespace DB
{
/**
 * An motivate example for this rewriter
 *     select a, n from ( select l.a, sum(r.b) as n  from t1 as l join t2 as r on l.a = r.a group by l.a ) where n > 100
 *
 * Rewrite the above sql to eliminate 'r.b', rename it as r_b.
 *     select a, n from (
 *         select l.a, sum(r_b) as n from (
 *             select l.a as a, r.b as r_b from t1 as l join t2 as r on l.a = r.a
 *         )as l group by l.a
 *     ) where n > 100
 *
 * Make the join into a nested sub-query.
 * If a column comes from the right table without an alias, make one alias for it.
 * If a column is the result of a function without an alias, make one alias for it.
 * This is for the convinience of making the join/groupby action could be run in distruted mode.
 */

class NestedJoinQueryRewriteAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = ASTPtr;
    explicit NestedJoinQueryRewriteAction(ContextPtr context_) : context(context_) {}
    ~NestedJoinQueryRewriteAction() override = default;

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
    SimpleVisitFrameStack<> frames;
    ContextPtr context;
    std::map<String, String> columns_alias;

    void visit(const ASTSelectWithUnionQuery * select_with_union_ast);
    void visit(const ASTSelectQuery * select_ast);
    void visit(const ASTTableExpression * table_expr_ast);
    void visit(const ASTSubquery * subquery_ast);

    void updateIdentNames(ASTSelectQuery * ast, ASTSelectQuery::Expression index);
    void renameSelectQueryIdentifiers(ASTSelectQuery * select_ast);
    void clearRenameAlias(const ASTSelectQuery * select_ast);

    static std::map<String, String> collectAliasColumns(const ASTExpressionList * select_expression);
};
}
