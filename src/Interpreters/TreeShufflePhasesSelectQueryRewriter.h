#pragma once
#include <memory>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTDistributedShuffleJoinSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>

namespace DB
{
class TreeShufflePhasesSelectQueryRewriterMatcher
{
public:
    using Visitor = InDepthNodeVisitor<TreeShufflePhasesSelectQueryRewriterMatcher, true>;
    struct Data
    {
        ContextPtr context;
        ASTPtr rewritten_query;
        size_t id_count = 0;
        std::map<UInt16, ASTs> shuffle_phases;
    };

    static void visit(ASTPtr & ast_, Data & data_);

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return false;
    }
private:
    static ASTPtr visit(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, UInt16 & frame_index);
    static ASTPtr visit(std::shared_ptr<ASTSelectWithUnionQuery> & query_, Data & data_, UInt16 & frame_index);
    static ASTPtr visitChild(ASTPtr & query_, Data & data_, UInt16 & frame_index);
    static ASTPtr visitSelectWithJoin(std::shared_ptr<ASTSelectQuery> & query_, Data & data_, UInt16 & frame_index);

    static std::shared_ptr<ASTTableExpression> visitJoinSelectTableExpression(const ASTTableExpression * table_expression, Data & data, UInt16 & frame_index);
    

};
using TreeShufflePhasesSelectQueryRewriterVistor = TreeShufflePhasesSelectQueryRewriterMatcher::Visitor;
}
