#include <memory>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/ASTRewriters/ASTBuildUtil.h>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/ASTRewriters/CollectQueryStoragesAction.h>
#include <Interpreters/ASTRewriters/CollectRequiredColumnsAction.h>
#include <Interpreters/ASTRewriters/IdentifierQualiferRemoveAction.h>
#include <Interpreters/ASTRewriters/StageQueryDistributedAggregationRewriteAction.h>
#include <Interpreters/ASTRewriters/StageQueryDistributedJoinRewriteAction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTStageQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Storages/DistributedShuffle/StorageShuffle.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionShuffle.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StageQueryDistributedAggregationRewriteAction::StageQueryDistributedAggregationRewriteAction(
    ContextPtr context_, ShuffleTableIdGeneratorPtr id_gen_)
    : context(context_), id_generator(id_gen_ ? id_gen_ : std::make_shared<ShuffleTableIdGenerator>())
{
}

void StageQueryDistributedAggregationRewriteAction::beforeVisitChildren(const ASTPtr & ast)
{
    LOG_TRACE(logger, "{} push frame. {}:{}", __LINE__, ast->getID(), queryToString(ast));
    frames.pushFrame(ast);
}

void StageQueryDistributedAggregationRewriteAction::afterVisitChild(const ASTPtr & /*ast*/)
{
    auto frame = frames.getTopFrame();
    auto result_ast = frame->result_ast;
    ASTs upstream_queries;
    frame->mergeChildrenUpstreamQueries();
    if (frame->upstream_queries.size() == 1)
        upstream_queries = frame->upstream_queries[0];
    frames.popFrame();
    frame = frames.getTopFrame();
    frame->children_results.emplace_back(result_ast);
    frame->addChildUpstreamQueries(upstream_queries);
}

ASTs StageQueryDistributedAggregationRewriteAction::collectChildren(const ASTPtr & ast)
{
    if (!ast)
        return {};
    LOG_TRACE(logger, "{} collectChildren. {}:{}", __LINE__, ast->getID(), queryToString(ast));
    ASTs children;
    if (const auto * union_select_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        children = union_select_ast->list_of_selects->children;
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        if (!select_ast->join())
        {
            children.emplace_back(getTableExpression(*select_ast, 0)->clone());
        }
        else
        {
            if (ASTAnalyzeUtil::hasAggregationColumn(ast) || ASTAnalyzeUtil::hasGroupBy(ast))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "There is join in the SELECT with aggregation columns. query: {}", queryToString(ast));
            }
        }
    }
    else if (const auto * table_expr_ast = ast->as<ASTTableExpression>())
    {
        if (table_expr_ast->subquery)
            children.emplace_back(table_expr_ast->subquery);
    }
    else if (const auto * subquery_ast = ast->as<ASTSubquery>())
    {
        children = subquery_ast->children;
    }
    return children;
}

void StageQueryDistributedAggregationRewriteAction::visit(const ASTPtr & ast)
{
    if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        visit(select_ast);
    }
    else if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        visit(select_with_union_ast);
    }
    else if (const auto * table_expr_ast = ast->as<ASTTableExpression>())
    {
        visit(table_expr_ast);
    }
    else if (const auto * subquery_ast = ast->as<ASTSubquery>())
    {
        visit(subquery_ast);
    }
    else
    {
        LOG_TRACE(logger, "Emptry action for ast({}): {}", ast->getID(), queryToString(ast));
        auto frame = frames.getTopFrame();
        frame->result_ast = frame->original_ast->clone();
    }
}

void StageQueryDistributedAggregationRewriteAction::visit(const ASTSelectWithUnionQuery * union_select_ast)
{
    auto frame = frames.getTopFrame();
    auto result_union_select_ast_ref = union_select_ast->clone();
    auto * result_union_select_ast = result_union_select_ast_ref->as<ASTSelectWithUnionQuery>();
    result_union_select_ast->list_of_selects->children = frame->children_results;

    if (frames.size() == 1)
    {
        frame->mergeChildrenUpstreamQueries();
        frame->result_ast = ASTStageQuery::make(result_union_select_ast_ref, frame->upstream_queries[0]);
    }
    else
        frame->result_ast = result_union_select_ast_ref;
}

void StageQueryDistributedAggregationRewriteAction::visit(const ASTTableExpression * table_expr_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = table_expr_ast->clone();
    auto * result_ast = frame->result_ast->as<ASTTableExpression>();
    if (!frame->children_results.empty())
    {
        result_ast->subquery = frame->children_results[0];
    }
}

void StageQueryDistributedAggregationRewriteAction::visit(const ASTSubquery * subquery_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = subquery_ast->clone();
    auto * result_subquery_ast = frame->result_ast->as<ASTSubquery>();
    result_subquery_ast->children = frame->children_results;
}

void StageQueryDistributedAggregationRewriteAction::visit(const ASTSelectQuery * select_ast)
{
    LOG_TRACE(logger, "{} frame size={}, select ast={}", __LINE__, frames.size(), queryToString(*select_ast));
    auto frame = frames.getTopFrame();
    if (frame->children_results.empty()) // join query
    {
        StageQueryDistributedJoinRewriteAction action(context, id_generator);
        ASTDepthFirstVisitor<StageQueryDistributedJoinRewriteAction> visitor(action, select_ast->clone());
        auto rewrite_ast = visitor.visit();

        auto * stage_query = rewrite_ast->as<ASTStageQuery>();
        if (!stage_query)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTStageQuery is expected. return query is : {}", queryToString(rewrite_ast));
        auto * return_select_ast = stage_query->current_query->as<ASTSelectQuery>();
        if (!return_select_ast)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTSelectQuery is expected. return query is :(id={}) {}", stage_query->current_query->getID(), queryToString(stage_query->current_query));


        ASTs upstream_queries;
        upstream_queries.insert(upstream_queries.end(), stage_query->upstream_queries.begin(), stage_query->upstream_queries.end());
        frame->upstream_queries = std::vector<ASTs>{upstream_queries};
        //frame->children_results.emplace_back(stage_query->current_query);
        frame->result_ast = stage_query->current_query;
    }
    else
    {
        if (ASTAnalyzeUtil::hasAggregationColumn(select_ast) || ASTAnalyzeUtil::hasGroupBy(select_ast))
        {
            if (!ASTAnalyzeUtil::hasGroupBy(select_ast))
            {
                visitSelectQueryWithAggregation(select_ast);
            }
            else
            {
                visitSelectQueryWithGroupby(select_ast);
            }
        }
        else
        {
            frame->result_ast = select_ast->clone();
            auto * result_ast = frame->result_ast->as<ASTSelectQuery>();
            auto * table_expr_ast = frame->children_results[0]->as<ASTTableExpression>();
            if (table_expr_ast->subquery)
            {
                auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*select_ast), context, true, true);
                String table_alias = tables_with_columns[0].table.alias;
                if (table_alias.empty() && !tables_with_columns[0].table.table.empty())
                    table_alias = tables_with_columns[0].table.table;

                table_expr_ast->subquery->as<ASTSubquery>()->setAlias(table_alias);
            }
            auto table_element = ASTBuildUtil::createTablesInSelectQueryElement(table_expr_ast);
            ASTBuildUtil::updateSelectQueryTables(result_ast, table_element->as<ASTTablesInSelectQueryElement>());
        }
    }

    if (frames.size() == 1)
    {
        LOG_TRACE(logger, "{} top select ast:{}", __LINE__, queryToString(*select_ast));
        frame->mergeChildrenUpstreamQueries();
        frame->result_ast = ASTStageQuery::make(frame->result_ast, frame->upstream_queries[0]);
    }
}

void StageQueryDistributedAggregationRewriteAction::visitSelectQueryWithAggregation(const ASTSelectQuery * select_ast)
{
    auto frame = frames.getTopFrame();
    auto * rewrite_table_expr =  frame->children_results[0]->as<ASTTableExpression>();
    if (!rewrite_table_expr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTTableExpression is expected. return query is : {}", queryToString(frame->children_results[0]));
    if (rewrite_table_expr->subquery)
    {
        auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*select_ast), context, true, true);
        String table_alias = tables_with_columns[0].table.alias;
        if (table_alias.empty() && !tables_with_columns[0].table.table.empty())
            table_alias = tables_with_columns[0].table.table;

        rewrite_table_expr->subquery->as<ASTSubquery>()->setAlias(table_alias);
    }

    CollectQueryStoragesAction collect_storage_action(context);
    ASTDepthFirstVisitor<CollectQueryStoragesAction> collect_storage_visitor(collect_storage_action, frame->children_results[0]);
    auto storages = collect_storage_visitor.visit();

    bool all_is_shuffle_storage = true;
    if (storages.size() == 2) // It should be a join query in the subquery
    {
        for (const auto & storage : storages)
        {
            if (storage->getName() != StorageShuffleJoin::NAME)
            {
                all_is_shuffle_storage = false;
                break;
            }
        }

        if (all_is_shuffle_storage)
        {
            auto tables = getDatabaseAndTablesWithColumns(getTableExpressions(*select_ast), context, true, true);
            if (tables.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Tables size should be 1");

            CollectRequiredColumnsAction collect_columns_action(tables);
            ASTDepthFirstVisitor<CollectRequiredColumnsAction> collect_columns_visitor(collect_columns_action, select_ast->clone());
            auto required_columns = collect_columns_visitor.visit().required_columns;

            auto insert_query = createShuffleInsert(
                TableFunctionLocalShuffle::name,
                rewrite_table_expr,
                ColumnWithDetailNameAndType::toNamesAndTypesList(required_columns[0]),
                nullptr);

            ASTs upstream_queries;
            frame->mergeChildrenUpstreamQueries();
            if (!frame->upstream_queries[0].empty())
                upstream_queries.emplace_back(ASTStageQuery::make(insert_query, frame->upstream_queries[0]));
            else
                upstream_queries.emplace_back(insert_query);
            frame->upstream_queries = std::vector<ASTs>{upstream_queries};

            auto table_function = insert_query->as<ASTInsertQuery>()->table_function->clone();
            table_function->as<ASTFunction>()->alias = ASTBuildUtil::getTableExpressionAlias(rewrite_table_expr);

            frame->result_ast = select_ast->clone();
            auto * result_select_ast = frame->result_ast->as<ASTSelectQuery>();
            ASTBuildUtil::updateSelectQueryTables(
                result_select_ast,
                ASTBuildUtil::createTablesInSelectQueryElement(table_function->as<ASTFunction>())->as<ASTTablesInSelectQueryElement>());
        }
    }
    else
    {
        all_is_shuffle_storage = false;
    }

    if (!all_is_shuffle_storage)
    {
        frame->result_ast = select_ast->clone();
        auto * result_ast = frame->result_ast->as<ASTSelectQuery>();
        ASTBuildUtil::updateSelectQueryTables(
            result_ast, ASTBuildUtil::createTablesInSelectQueryElement(rewrite_table_expr)->as<ASTTablesInSelectQueryElement>());

    }
}

void StageQueryDistributedAggregationRewriteAction::visitSelectQueryWithGroupby(const ASTSelectQuery * select_ast)
{
    LOG_TRACE(logger, "visitSelectQueryWithGroupby:{}", queryToString(*select_ast));
    auto frame = frames.getTopFrame();
    auto tables = getDatabaseAndTablesWithColumns(getTableExpressions(*select_ast), context, true, true);
    if (tables.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tables size should be 1");
    
    auto * rewrite_table_expr =  frame->children_results[0]->as<ASTTableExpression>();
    if (!rewrite_table_expr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTTableExpression is expected. return query is : {}", queryToString(frame->children_results[0]));
    if (rewrite_table_expr->subquery)
    {
        auto tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*select_ast), context, true, true);
        String table_alias = tables_with_columns[0].table.alias;
        if (table_alias.empty() && !tables_with_columns[0].table.table.empty())
            table_alias = tables_with_columns[0].table.table;

        rewrite_table_expr->subquery->as<ASTSubquery>()->setAlias(table_alias);
    }
    
    if (isAllRequiredColumnsLowCardinality(select_ast->groupBy(), tables))
    {
        CollectQueryStoragesAction collect_storage_action(context);
        ASTDepthFirstVisitor<CollectQueryStoragesAction> collect_storage_visitor(collect_storage_action, frame->children_results[0]);
        auto storages = collect_storage_visitor.visit();
        /// If all columns used in the groupby clasue are low cardinality, do not shuffle the data and 
        /// run the groupby in the two-phase way.
        if (storages.size() > 1)
            visitSelectQueryWithAggregation(select_ast);
        return;
    }

    CollectRequiredColumnsAction collect_columns_action(tables);
    ASTDepthFirstVisitor<CollectRequiredColumnsAction> collect_columns_visitor(collect_columns_action, select_ast->clone());
    auto required_columns = collect_columns_visitor.visit().required_columns;

    ASTPtr insert_query = createShuffleInsert(
        TableFunctionShuffleAggregation::name,
        rewrite_table_expr,
        ColumnWithDetailNameAndType::toNamesAndTypesList(required_columns[0]),
        select_ast->groupBy());

    auto * insert_query_ptr = insert_query->as<ASTInsertQuery>();
    auto * insert_select_ptr = insert_query_ptr->select->as<ASTSelectWithUnionQuery>()->list_of_selects->children[0]->as<ASTSelectQuery>();
    IdentifiterQualiferRemoveAction remove_qualifier_action;
    ASTDepthFirstVisitor<IdentifiterQualiferRemoveAction> remove_qualifier_visitor(remove_qualifier_action, select_ast->where());
    auto where_expr = remove_qualifier_visitor.visit();
    insert_select_ptr->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expr));

    ASTs upstream_queries;
    frame->mergeChildrenUpstreamQueries();
    if (!frame->upstream_queries[0].empty())
        upstream_queries.emplace_back(ASTStageQuery::make(insert_query, frame->upstream_queries[0]));
    else
        upstream_queries.emplace_back(insert_query);
    frame->upstream_queries = std::vector<ASTs>{upstream_queries};

    auto table_function = insert_query->as<ASTInsertQuery>()->table_function->clone();
    table_function->as<ASTFunction>()->alias = ASTBuildUtil::getTableExpressionAlias(rewrite_table_expr);

    frame->result_ast = select_ast->clone();
    auto * result_select_ast = frame->result_ast->as<ASTSelectQuery>();
    result_select_ast->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
    ASTBuildUtil::updateSelectQueryTables(
        result_select_ast,
        ASTBuildUtil::createTablesInSelectQueryElement(table_function->as<ASTFunction>())->as<ASTTablesInSelectQueryElement>());
}

ASTPtr StageQueryDistributedAggregationRewriteAction::createShuffleInsert(
    const String & table_function_name, ASTTableExpression * table_expr, const NamesAndTypesList & table_desc, ASTPtr groupby_clause)
{
    ASTPtr hash_expr;
    if (groupby_clause)
    {
        IdentifiterQualiferRemoveAction remove_qualifier_action;
        ASTDepthFirstVisitor<IdentifiterQualiferRemoveAction> remove_qualifier_visitor(remove_qualifier_action, groupby_clause);
        hash_expr = remove_qualifier_visitor.visit();
    }

    auto session_id = context->getClientInfo().current_query_id;
    auto cluster_name = context->getSettings().use_cluster_for_distributed_shuffle.value;
    auto table_id = getNextId();

    auto table_function
        = ASTBuildUtil::createShuffleTableFunction(table_function_name, cluster_name, session_id, table_id, table_desc, hash_expr);

    auto select_query_ref = std::make_shared<ASTSelectQuery>();
    auto * select_query = select_query_ref->as<ASTSelectQuery>();
    ASTBuildUtil::updateSelectQueryTables(select_query, table_expr);
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, ASTBuildUtil::createSelectExpression(table_desc));

    return ASTBuildUtil::createTableFunctionInsertSelectQuery(table_function, ASTBuildUtil::wrapSelectQuery(select_query));
}

bool StageQueryDistributedAggregationRewriteAction::isAllRequiredColumnsLowCardinality(const ASTPtr & ast, const TablesWithColumns & tables)
{
    CollectRequiredColumnsAction collect_columns_action(tables);
    ASTDepthFirstVisitor<CollectRequiredColumnsAction> collect_columns_visitor(collect_columns_action, ast);
    auto required_columns = collect_columns_visitor.visit().required_columns;
    for (auto & cols : required_columns)
    {
        for (auto & col : cols)
        {
            LOG_TRACE(logger, "check group by col. {} {}", col.full_name, col.type->getName());
            if (!col.type->lowCardinality())
                return false;
        }
    }
    LOG_TRACE(logger, "all columns are locaCardinality");
    return true;

}
}
