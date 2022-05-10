#pragma once
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
namespace DB
{
class StageQueryDistributedJoinRewriteAnalyzer
{
public:
    struct Result
    {
        // for descrite table structure
        std::vector<NamesAndTypesList> tables_columns;
        // each is ASTExpressionList, containes all required columns for hashing
        ASTs tables_hash_keys;
    };
    explicit StageQueryDistributedJoinRewriteAnalyzer(const ASTSelectQuery * query_, ContextPtr context_);

    // If this query cannot be rewrite under this strategy, return empty
    std::optional<Result> analyze();
private:
    const ASTSelectQuery * from_query;
    ContextPtr context;
    TablesWithColumns tables_with_columns;
    std::vector<ColumnWithDetailNameAndTypes> tables_columns_from_select;
    std::vector<ColumnWithDetailNameAndTypes> tables_columns_from_on_join;
    ASTs tables_hash_keys;
    std::vector<NamesAndTypesList> tables_columns;

    Poco::Logger * logger = &Poco::Logger::get("StageQueryDistributedJoinRewriteAnalyzer");

    bool isApplicableJoinType();

    bool collectHashKeys();
    bool collectHashKeysOnEqual(ASTPtr ast, ASTs & keys, const std::map<String, ASTPtr> & alias_columns);
    bool collectHashKeysOnAnd(ASTPtr ast, ASTs & keys, const std::map<String, ASTPtr> & alias_columns);
    void collectTablesColumns();
};
class StageQueryDistributedJoinRewriteAction : public EmptyASTDepthFirstVisitAction
{
public:
    struct Frame : public SimpleVisitFrame
    {
        explicit Frame(const ASTPtr & ast) : SimpleVisitFrame(ast) {}
        std::vector<ASTs> upstream_queries;

        void addChildUpstreamQueries(const ASTs & queries)
        {
            upstream_queries.emplace_back(queries);
        }

        void mergeChildrenUpstreamQueries()
        {
            ASTs result_upstream_queris;
            for (auto & queries : upstream_queries)
                result_upstream_queris.insert(result_upstream_queris.end(), queries.begin(), queries.end());
            upstream_queries = std::vector<ASTs>{result_upstream_queris};
        }
    };

    using Result = ASTPtr;
    explicit StageQueryDistributedJoinRewriteAction(ContextPtr context_, ShuffleTableIdGeneratorPtr id_gen_ = nullptr);
    ~StageQueryDistributedJoinRewriteAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override;
    void beforeVisitChildren(const ASTPtr & ast) override;
    void afterVisitChild(const ASTPtr & ast) override;
    void visit(const ASTPtr & ast);

    inline Result getResult()
    {
        return frames.getTopFrame()->result_ast;
    }

private:
    ContextPtr context;
    ShuffleTableIdGeneratorPtr id_generator;
    SimpleVisitFrameStack<Frame> frames;
    Poco::Logger * logger = &Poco::Logger::get("StageQueryDistributedJoinRewriteAction");


    void visit(const ASTSelectWithUnionQuery * select_with_union_ast);
    void visit(const ASTSelectQuery * select_ast);
    void visitSelectQueryWithAggregation(const ASTSelectQuery * select_ast);
    void visitSelectQueryOnJoin(const ASTSelectQuery * select_ast);
    void visit(const ASTTableExpression * table_expr_ast);
    void visit(const ASTSubquery * subquery_ast);

    ASTPtr createShuffleInsertForJoin(const String & table_id,
        ASTTableExpression * table_expr,
        const NamesAndTypesList & table_desc,
        const ASTPtr & hash_expr);


    String getNextTableId()
    {
        static const String prefix = "join_";
        return prefix + std::to_string(id_generator->nextId());
    }
};
}
