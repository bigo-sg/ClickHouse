#pragma once
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTTreeQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTTreeQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
namespace DB
{
class TreeQueryAggregationRewriter
{
public:
    explicit TreeQueryAggregationRewriter(ContextPtr context_, ASTPtr ast_);
    ASTPtr run();
private:
    ContextPtr context;
    ASTPtr from_ast;
    size_t id_count = 0;
    Poco::Logger * logger = &Poco::Logger::get("TreeQueryAggregationRewriter");

    ASTPtr visitChild(IAST * query, UInt32 depth, ASTs & children);
    ASTPtr visit(ASTSelectQuery * query, UInt32 depth, ASTs & children);
    ASTPtr visitSelectWithJoin(ASTSelectQuery * query, UInt32 depth, ASTs & children);
    static ASTPtr visitOnlyAggregation(ASTSelectQuery * query, UInt32 depth, ASTs & children);
    ASTPtr visitGroupBy(ASTSelectQuery * query, UInt32 depth, ASTs & children);
    ASTPtr visit(ASTSelectWithUnionQuery * query, UInt32 depth, ASTs & children);
    ASTPtr visit(ASTInsertQuery * query, UInt32 depth, ASTs & children);
    ASTPtr visit(ASTTreeQuery * query, UInt32 depth, ASTs & children);

    ASTPtr makeInsertSelectQuery(const ColumnWithDetailNameAndTypes & required_columns, const ASTSelectQuery * original_select_query, const ASTExpressionList * groupby);

    ASTPtr makeInsertTableFunction(const String & sesson_id, const String & id, const String & table_structure, const String & hash_expression_list);

    inline String getNextId()
    {
        static String prefix = "agg_table_";
        return prefix + std::to_string(id_count++); 
    }
};

}
