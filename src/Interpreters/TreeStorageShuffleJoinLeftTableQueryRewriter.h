#pragma  once
#include <memory>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include "Parsers/IAST_fwd.h"
namespace DB
{
/**
 */

class TreeStorageShuffleJoinLeftTableQueryRewriteMatcher
{
public:
    using Visitor = InDepthNodeVisitor<TreeStorageShuffleJoinLeftTableQueryRewriteMatcher, true>;
    struct Data
    {
        ContextPtr context;
        String session_id;
        String table_id;
        ASTPtr rewritten_query;
    };

    static void visit(ASTPtr & ast_, Data & data_);
    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return false;
    }
private:
    static ASTPtr visit(std::shared_ptr<ASTSelectWithUnionQuery> query, Data & data_);
    static ASTPtr visit(std::shared_ptr<ASTSelectQuery> query, Data & data_);
};
using TreeStorageShuffleJoinLeftTableQueryRewriterVisitor = TreeStorageShuffleJoinLeftTableQueryRewriteMatcher::Visitor;
}
