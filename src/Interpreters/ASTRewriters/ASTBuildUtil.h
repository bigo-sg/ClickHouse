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
     * @brief Create a Shuffle Table Function object
     *
     * @param function_name which shuffle function to use. see TableFunctionShuffle.h
     * @param session_id session_od
     * @param cluster_name cluster name
     * @param table_id table_id
     * @param columns describe the table structure. etc. 'x int, y string'
     * @param hash_expression_list  hash expression list for shuffle hashing. etc. 'x, y'
     * @param alias table alias
     * @return ASTPtr ASTFunction
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
     * @brief Create a Table Function Insert Select Query object
     *
     * @param table_function must be a ASTFunction
     * @param select_query must be a ASTSelectWithUnionQuery
     * @return ASTPtr it's a ASTInsertQuery
     */
    static ASTPtr createTableFunctionInsertSelectQuery(ASTPtr table_function, ASTPtr select_query);

    /**
     * @brief Create a ASTSelectWithUnionQuery with a ASTSelectQuery
     *
     * @param select_query must be a ASTSelectQuery
     * @return ASTPtr it's a ASTSelectWithUnionQuery
     */
    static ASTPtr wrapSelectQuery(const ASTSelectQuery * select_query);

    /**
     * @brief Create a Select Expression object
     *
     * @param names_and_types Use the names to build the select expression
     * @return ASTPtr
     */
    static ASTPtr createSelectExpression(const NamesAndTypesList & names_and_types);

    /**
     * @brief Update ASTSelectQuery::TABLES ASTTableExpressions
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
