#pragma once
#include <memory>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Parsers/ASTStagedSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>
namespace DB
{
class ASTSelectQuery;
#if 0
/**
 * for breaking select query with join clause into multi stage query
 * 
 */    
class MultiStageTreeReWriter : WithContext
{
public:
    struct RewriteResult
    {
        ASTPtr final_ast;
    };
    using RewriteResultPtr = std::unique_ptr<RewriteResult>;
    explicit MultiStageTreeReWriter(ContextPtr context_) : WithContext(context_){}

    RewriteResultPtr analyze(ASTPtr query);   
private: 
    bool getTablesHeader(const ASTSelectQuery & select_query_, ASTPtr & left_select_, ASTPtr & right_select_);
    bool getTablesHashKeys(ASTPtr select_query_);
    Poco::Logger * logger = &Poco::Logger::get("MultiStageTreeReWriter");
}; 
#endif
class DistributedJoinRewriter : WithContext
{
public:
    explicit DistributedJoinRewriter(std::shared_ptr<ASTSelectQuery> query_, ContextPtr context_, size_t assigned_id_);
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

    std::shared_ptr<ASTInsertQuery> createSubJoinTable(const Strings & table_id, std::shared_ptr<ASTTableExpression> table_expression);
private:
    friend class DistributedJoinRewriteMatcher;
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

    ASTPtr createNewJoinSelectQuery(ASTPtr & left_query, ASTPtr & right_query);
    ASTPtr createLeftTableQuery(const String & hash_table_id);
    ASTPtr createRightTableQuery(const String & hash_table_id);
    ASTPtr createHashTableExpression(const String & table_id, NamesAndTypesList & columns, ASTPtr & hash_keys);
    ASTPtr createHashTableExpression(const Strings & table_id, NamesAndTypesList & columns, ASTPtr & hash_keys);

};

class DistributedJoinRewriteMatcher
{
public:
    using Visitor = InDepthNodeVisitor<DistributedJoinRewriteMatcher, true>;
    struct VisitFrame
    {

    };
    struct Data
    {
        ContextPtr context;
        ASTPtr rewritten_query;
        //std::shared_ptr<ASTStagedSelectQuery> stage_query;
        size_t id_count = 0;
    };

    static void visit(ASTPtr & ast_, Data & data_);

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return false;
    }

private:
    static ASTPtr visit(std::shared_ptr<ASTSelectQuery> & query_, Data & data_);
    static ASTPtr visit(std::shared_ptr<ASTSelectWithUnionQuery> & query_, Data & data_);
    static ASTPtr visitChild(ASTPtr & query_, Data & data_);
    static ASTPtr visitSelectWithJoin(std::shared_ptr<ASTSelectQuery> & query_, Data & data_);

    static std::shared_ptr<ASTTableExpression> visitJoinSelectTableExpression(const ASTTableExpression * table_expression, Data & data);
};

using DistributedJoinRewriteVisitor = DistributedJoinRewriteMatcher::Visitor;
}
