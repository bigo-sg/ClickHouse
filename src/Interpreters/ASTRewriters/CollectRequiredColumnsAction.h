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
#include <arrow/result.h>
#include <Parsers/ASTOrderByElement.h>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
namespace DB
{
class CollectRequiredColumnsAction : public EmptyASTDepthFirstVisitAction
{
public:
    struct Result
    {
        std::vector<ColumnWithDetailNameAndTypes> required_columns;
    };

    explicit CollectRequiredColumnsAction(const TablesWithColumns & tables_, const std::map<String, ASTPtr> alias_asts_ = {})
        : tables(tables_), alias_asts(alias_asts_)
    {
        for (size_t i = 0; i < tables.size(); ++i)
        {
            final_result.required_columns.emplace_back(ColumnWithDetailNameAndTypes{});
        }
    }
    ~CollectRequiredColumnsAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override;
    void beforeVisitChild(const ASTPtr & ast) override;
    void afterVisitChild(const ASTPtr & ast) override;
    void visit(const ASTPtr & ast);

    Result getResult() const { return final_result; }

private:
    const TablesWithColumns & tables;
    Result final_result;
    std::set<String> added_names; // not need to filled by outside;
    std::set<String> alias_names;
    std::map<String, ASTPtr> alias_asts; // asts with alias in the select expression list

    void visit(const ASTFunction * function_ast);
    void visit(const ASTIdentifier * ident_ast);
    void visit(const ASTAsterisk * asterisk_ast);
    void visit(const ASTQualifiedAsterisk * qualified_asterisk);
};
}
