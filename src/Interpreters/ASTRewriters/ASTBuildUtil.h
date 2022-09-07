#pragma once
#include <Core/NamesAndTypes.h>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
namespace DB
{
class ASTBuildUtil
{
public:
    static String getTableExpressionAlias(const ASTTableExpression * ast);
    static std::shared_ptr<ASTExpressionList> toShortNameExpressionList(const ColumnWithDetailNameAndTypes & columns);
    static String toTableStructureDescription(const ColumnWithDetailNameAndTypes & columns);

    /**
     * Create a Shuffle Table Function object
     *
     * - function_name which shuffle function to use. see TableFunctionShuffle.h
     * - session_id session_od
     * - cluster_name cluster name
     * - table_id table_id
     * - columns describe the table structure. etc. 'x int, y string'
     * - hash_expression_list  hash expression list for shuffle hashing. etc. 'x, y'
     * - alias table alias
     */
    static ASTPtr createShuffleTableFunction(
        const String & function_name,
        const String & cluster_name,
        const String & session_id,
        const String & table_id,
        const NamesAndTypesList & columns,
        const ASTPtr & hash_expression_list,
        const String & alias = "");

    /**
     * Create a Table Function Insert Select Query object
     *
     * - table_function must be a ASTFunction
     * - select_query must be a ASTSelectWithUnionQuery
     * return ASTPtr it's a ASTInsertQuery
     */
    static ASTPtr createTableFunctionInsertSelectQuery(ASTPtr table_function, ASTPtr select_query);

    /**
     * Create a ASTSelectWithUnionQuery with a ASTSelectQuery
     *
     * - select_query must be a ASTSelectQuery
     * return ASTPtr it's a ASTSelectWithUnionQuery
     */
    static ASTPtr wrapSelectQuery(const ASTSelectQuery * select_query);

    /**
     * Create a Select Expression object
     *
     * - names_and_types Use the names to build the select expression
     * return ASTPtr
     */
    static ASTPtr createSelectExpression(const NamesAndTypesList & names_and_types);

    /**
     * Update ASTSelectQuery::TABLES ASTTableExpressions
     */
    static void updateSelectQueryTables(ASTSelectQuery * select_query, const ASTTableExpression * table_expr_);

    static void updateSelectQueryTables(ASTSelectQuery * select_query, const ASTTablesInSelectQueryElement * table_element_);

    static void updateSelectQueryTables(
        ASTSelectQuery * select_query,
        const ASTTablesInSelectQueryElement * left_table_element_,
        const ASTTablesInSelectQueryElement * right_table_element_);

    static ASTPtr createTablesInSelectQueryElement(const ASTTableExpression * table_expr_, ASTPtr table_join_ = nullptr);
    static ASTPtr createTablesInSelectQueryElement(const ASTFunction * func_, ASTPtr table_join_ = nullptr);
    static ASTPtr createTablesInSelectQueryElement(const ASTSelectWithUnionQuery * select_query, const String & alias = "");

};

}
