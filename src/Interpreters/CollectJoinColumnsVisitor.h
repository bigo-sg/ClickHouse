#pragma once
#include <Core/Names.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Aliases.h>

namespace DB
{
class ASTIdentifier;
class CollectJoinColumnsMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<CollectJoinColumnsMatcher, true>;

    struct Data
    {
        const TablesWithColumns & tables;
        std::vector<NamesAndTypesList> & required_columns;
    };

    static void visit(const ASTPtr & ast_, Data & data_);

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
private:
    static void visit(const ASTFunction & func_, const ASTPtr & ast_, Data & data_);
    static void visit(const ASTIdentifier & ident_, const ASTPtr & ast_, Data & data_);

    static const ASTIdentifier * unrollAliases(const ASTIdentifier * ident_, const Aliases & aliases_);
};

using CollectJoinColumnsVisitor = CollectJoinColumnsMatcher::Visitor;
}
