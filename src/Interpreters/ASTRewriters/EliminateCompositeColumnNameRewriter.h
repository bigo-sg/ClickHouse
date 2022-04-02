#pragma  once
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
namespace DB
{
/**
 * An example for this rewriter's motivation
 *     select a, n from ( select l.a, sum(r.b) as n  from t1 as l join t2 as r on l.a = r.a group by l.a ) where n > 100
 *
 * Rewrite the above sql to eliminate 'r.b', rename it as r_b. 
 *     select a, n from (
 *         select l.a, sum(r_b) as n from (
 *             select l.a as a, r.b as r_b from t1 as l join t2 as r on l.a = r.a
 *         )as l group by l.a
 *     ) where n > 100
 *
 * This rewrite action only is token when a join follow by a group by  
 */

class EliminateCompositeColumnNameRewriter
{
public:
    explicit EliminateCompositeColumnNameRewriter(ContextPtr context_, ASTPtr ast_);
    ASTPtr run();

private:
    ContextPtr context;
    ASTPtr from_ast;
    std::map<String, size_t> function_alias_id;
    std::map<String, String> columns_alias;
    Poco::Logger * logger = &Poco::Logger::get("EliminateCompositeColumnNameRewriter");

    ASTPtr visitChild(IAST * ast);
    ASTPtr visit(ASTSelectWithUnionQuery * ast);
    ASTPtr visit(ASTSelectQuery * ast);
    ASTPtr visitJoinedSelect(ASTSelectQuery * ast);
    static std::shared_ptr<ASTTableExpression> visitJoinedTableExpression(const ASTTableExpression * table_expression);
    ASTPtr visit(ASTInsertQuery * ast);
    
    String getFunctionAlias(const String & function_name);

    std::map<String, String> collectAliasColumns(ASTExpressionList * select_expression);

    void updateIdentNames(ASTSelectQuery * ast, ASTSelectQuery::Expression index, bool rename_function = false);
    void renameSelectQueryIdentifiers(ASTSelectQuery * select_ast);
    void clearRenameAlias(ASTSelectQuery * select_ast);

    void printColumnAlias();
};
}
