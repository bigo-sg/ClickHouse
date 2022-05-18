#pragma once
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{
class StageQueryDistributedAggregationRewriteAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = ASTPtr;

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

    explicit StageQueryDistributedAggregationRewriteAction(ContextPtr context_, ShuffleTableIdGeneratorPtr id_gen_ = nullptr);
    ~StageQueryDistributedAggregationRewriteAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override;
    void beforeVisitChildren(const ASTPtr & ast) override;
    void afterVisitChild(const ASTPtr & ast) override;

    void visit(const ASTPtr & ast);

    Result getResult() { return frames.getTopFrame()->result_ast; }

private:
    ContextPtr context;
    ShuffleTableIdGeneratorPtr id_generator;
    SimpleVisitFrameStack<Frame> frames;
    Poco::Logger * logger = &Poco::Logger::get("StageQueryDistributedAggregationRewriteAction");


    void visit(const ASTSelectWithUnionQuery * union_select_ast);
    void visit(const ASTSelectQuery * select_ast);
    void visitSelectQueryWithGroupby(const ASTSelectQuery * select_ast);
    void visitSelectQueryWithAggregation(const ASTSelectQuery * select_ast);
    void visit(const ASTTableExpression * table_expr_ast);
    void visit(const ASTSubquery * subquery_ast);

    String getNextId()
    {
        static const String prefix = "agg_";
        return prefix + std::to_string(id_generator->nextId());
    }

    ASTPtr createShuffleInsert(
        const String & table_function_name, ASTTableExpression * table_expr, const NamesAndTypesList & table_desc, ASTPtr groupby_clause);

    bool isAllRequiredColumnsLowCardinality(const ASTPtr & ast, const TablesWithColumns & tables);
};
}
