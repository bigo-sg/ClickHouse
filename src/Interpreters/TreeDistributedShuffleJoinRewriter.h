#pragma once
#include <memory>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Parsers/ASTDistributedShuffleJoinSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>
namespace DB
{
class ASTSelectQuery;
class TreeDistributedShuffleJoinRewriter : WithContext
{
public:
    explicit TreeDistributedShuffleJoinRewriter(std::shared_ptr<ASTSelectQuery> query_, ContextPtr context_, size_t assigned_id_);
    ASTPtr rewrite();
    bool prepare();
    inline std::vector<String> getHashTableId(const String & left_or_right)
    {
        std::vector<String> res;
        res.emplace_back(getContext()->getClientInfo().current_query_id);
        res.emplace_back(std::to_string(assigned_id) + "_" + left_or_right);
        return res;
    }

    ASTPtr createNewJoinSelectQuery(const Strings & left_table_id, const Strings & right_table_id);
    inline const NamesAndTypesList & getSubTableColumns(size_t idx)
    {
        return tables_columns[idx];
    }
    inline ASTPtr getSubTableHashKeys(size_t idx)
    {
        return tables_hash_keys_list[idx];
    }

    std::shared_ptr<ASTInsertQuery> createSubJoinTable(const Strings & table_id, std::shared_ptr<ASTTableExpression> table_expression, UInt32 left_or_right);
private:
    friend class TreeDistributedShuffleJoinRewriteMatcher;
    JoinedTables joined_tables;
    std::shared_ptr<ASTSelectQuery> query;
    size_t assigned_id;
    const ASTTablesInSelectQueryElement * joined_elements;
    TablesWithColumns tables_with_columns;
    std::vector<NamesAndTypesList> tables_columns_from_select;
    std::vector<NamesAndTypesList> tables_columns_from_on_join;
    std::vector<NamesAndTypesList> tables_columns;
    ASTs tables_hash_keys_list;

    bool checkRewritable();

    bool collectTablesColumnsFromSelect();
    bool collectTablesColumnsFromOnJoin();
    bool collectTablesColumns();

    bool collectHashKeysList();
    bool collectHashKeysListOnEqual(ASTPtr ast, ASTs & keys_list);
    bool collectHashKeysListOnAnd(ASTPtr ast, ASTs & keys_list);
    //void rewriteHashKeysIdentifier(ASTPtr ast, const TableWithColum);

    ASTPtr createNewJoinSelectQuery(ASTPtr & left_query, ASTPtr & right_query);
    ASTPtr createLeftTableQuery(const String & hash_table_id);
    ASTPtr createRightTableQuery(const String & hash_table_id);
    static ASTPtr createHashTableExpression(const String & table_id, NamesAndTypesList & columns, ASTPtr & hash_keys);
    ASTPtr createHashTableExpression(const Strings & table_id, NamesAndTypesList & columns, ASTPtr & hash_keys);

};

}
