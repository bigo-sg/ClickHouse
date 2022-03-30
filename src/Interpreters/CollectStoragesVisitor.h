#pragma once
#include <memory>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
class CollectStoragesMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<CollectStoragesMatcher, true>;
    struct Data
    {
        ContextPtr context;
        std::vector<StoragePtr> storages;
    };
    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return false;
    }
private:
    static void visit(const std::shared_ptr<ASTSelectWithUnionQuery> & ast, Data & data);
    static void visit(const std::shared_ptr<ASTSelectQuery> & ast, Data & data);
    //static void visitChild(ASTPtr ast, Data & data);
};
using CollectStoragesVisitor = CollectStoragesMatcher::Visitor;
}
