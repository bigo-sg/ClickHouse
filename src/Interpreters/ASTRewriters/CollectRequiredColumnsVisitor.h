#pragma once
#include <vector>
#include <Core/Names.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Aliases.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
namespace DB
{
// collect all required columns from a select query.
class CollectRequiredColumnsMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<CollectRequiredColumnsMatcher, true>;

    struct Data
    {
        const TablesWithColumns & tables;
        std::vector<ColumnWithDetailNameAndTypes> & required_columns;// required columns from differrent table
        std::set<String> added_names; // not need to filled by outside;
        std::set<String> alias_names;
    };

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return false;
        //return !parent->as<ASTSelectQuery>() && !parent->as<ASTIdentifier>();
    }

private:
    static void visit(const ASTSelectQuery & select_ast, Data & data);
    static void visit(const ASTExpressionList & expression_list, Data & data);
    static void visit(const ASTFunction & func_ast, Data & data);
    static void visit(const ASTIdentifier & ident, Data & data);

};
using CollectRequiredColumnsVisitor = CollectRequiredColumnsMatcher::Visitor;
}
