#pragma once
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTTreeQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTTreeQuery.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Poco/Logger.h>
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

    static ASTPtr visitChild(IAST * query, UInt32 depth, ASTs & children);
    static ASTPtr visit(ASTSelectQuery * query, UInt32 depth, ASTs & children);
    static ASTPtr visitSelectWithJoin(ASTSelectQuery * query, UInt32 depth, ASTs & children);
    static ASTPtr visit(ASTSelectWithUnionQuery * query, UInt32 depth, ASTs & children);
    static ASTPtr visit(ASTInsertQuery * query, UInt32 depth, ASTs & children);
    static ASTPtr visit(ASTTreeQuery * query, UInt32 depth, ASTs & children);

    inline String getNextId()
    {
        static String prefix = "agg_table_";
        return prefix + std::to_string(id_count++); 
    }
};

}
