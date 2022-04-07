#pragma once
#include <memory>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTTreeQuery.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>
#include "Interpreters/DatabaseCatalog.h"
#include "Parsers/IAST_fwd.h"
#include "base/types.h"

namespace DB
{
class TreeQueryJoinRewriter
{
public:
    explicit TreeQueryJoinRewriter(ContextPtr context_, ASTPtr ast_);
    ASTPtr run();
private:
    ContextPtr context;
    ASTPtr from_ast;
    size_t table_id_count = 0;
    Poco::Logger * logger = &Poco::Logger::get("TreeQueryJoinRewriter");
    inline size_t getNextTableId() { return table_id_count++; }

    ASTPtr visitChild(IAST * ast, size_t depth, ASTs & children);
    ASTPtr visit(ASTSelectQuery * ast, size_t depth, ASTs & children);
    ASTPtr visitJoinedSelect(ASTSelectQuery * ast, size_t depth, ASTs & children);
    std::shared_ptr<ASTTableExpression> visitJoinedTableExpression(const ASTTableExpression * table_expression, ASTs & children);
    ASTPtr visit(ASTSelectWithUnionQuery * ast, size_t depth, ASTs & children);


};

}
