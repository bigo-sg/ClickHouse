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

namespace DB
{
class TreeQueryJoinRewriterMatcher
{
public:
    using Visitor = InDepthNodeVisitor<TreeQueryJoinRewriterMatcher, true>;
    
    struct Data
    {
        ContextPtr context;
        ASTPtr rewritten_query;
        size_t id_count = 0;
    };

    static void visit(ASTPtr & ast_, Data & data_);
    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return false;
    }
private:
    static ASTPtr visit(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, size_t visit_depth_, ASTs & children);
    static ASTPtr visit(std::shared_ptr<ASTSelectWithUnionQuery> & query_, Data & data_, size_t visit_depth_, ASTs & children);
    static ASTPtr visitChild(ASTPtr & query_, Data & data_, size_t visit_depth_, ASTs & children);
    static ASTPtr visitSelectWithJoin(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, size_t visit_depth_, ASTs & children);

    static std::shared_ptr<ASTTableExpression> visitJoinSelectTableExpression(const ASTTableExpression * table_expression, Data & data, ASTs & children);

    static std::shared_ptr<ASTTreeQuery> makeTreeQuery(ASTPtr output_query, const ASTs & input_queries);
};
using TreeQueryJoinRewriterVisitor = TreeQueryJoinRewriterMatcher::Visitor;
}
