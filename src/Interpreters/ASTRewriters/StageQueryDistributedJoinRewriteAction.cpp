#include <memory>
#include <type_traits>
#include <Interpreters/ASTRewriters/ASTAnalyzeUtil.h>
#include <Interpreters/ASTRewriters/ASTBuildUtil.h>
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/ASTRewriters/CollectAliasColumnElementsAction.h>
#include <Interpreters/ASTRewriters/CollectRequiredColumnsAction.h>
#include <Interpreters/ASTRewriters/IdentifierQualiferRemoveAction.h>
#include <Interpreters/ASTRewriters/StageQueryDistributedAggregationRewriteAction.h>
#include <Interpreters/ASTRewriters/StageQueryDistributedJoinRewriteAction.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTStageQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <TableFunctions/TableFunctionShuffle.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageDictionary.h>
#include <Interpreters/JoinedTables.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StageQueryDistributedJoinRewriteAction::StageQueryDistributedJoinRewriteAction(ContextPtr context_, ShuffleTableIdGeneratorPtr id_gen_)
    : context(context_)
    , id_generator(id_gen_ ? id_gen_ : std::make_shared<ShuffleTableIdGenerator>())
{}

void StageQueryDistributedJoinRewriteAction::beforeVisitChildren(const ASTPtr & ast)
{
    frames.pushFrame(ast);
}

void StageQueryDistributedJoinRewriteAction::afterVisitChild(const ASTPtr & /*ast*/)
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
    //frame->upstream_queries.insert(frame->upstream_queries.end(), upstream_queries.begin(), upstream_queries.end());

}

ASTs StageQueryDistributedJoinRewriteAction::collectChildren(const ASTPtr & ast)
{
    if (!ast)
        return {};

    ASTs children;
    if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        if (ASTAnalyzeUtil::hasAggregationColumn(ast) || ASTAnalyzeUtil::hasGroupBy(ast))
        {
            if (select_ast->join())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "There is join in the SELECT with aggregation columns. query: {}", queryToString(ast));
            }
        }
        else
        {
            if (const auto * left_table_expr = getTableExpression(*select_ast, 0))
                children.emplace_back(left_table_expr->clone());
            if (const auto * right_table_expr = getTableExpression(*select_ast, 1))
                children.emplace_back(right_table_expr->clone());
        }

    }
    else if (const auto * select_with_union_ast = ast->as<ASTSelectWithUnionQuery>())
    {
        children = select_with_union_ast->list_of_selects->children;
    }
    else if (const auto * table_expr_ast = ast->as<ASTTableExpression>())
    {
        if (table_expr_ast->subquery)
            children.emplace_back(table_expr_ast->subquery);
    }
    else if (const auto * subquery_ast = ast->as<ASTSubquery>())
    {
        children.emplace_back(subquery_ast->children[0]);
    }
    else
    {
        LOG_TRACE(logger, "Return empty children for ast({}): {}", ast->getID(), queryToString(ast));
    }
    return children;
}

void StageQueryDistributedJoinRewriteAction::visit(const ASTPtr & ast)
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

void StageQueryDistributedJoinRewriteAction::visit(const ASTTableExpression * table_expr_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = table_expr_ast->clone();
    auto * result_ast = frame->result_ast->as<ASTTableExpression>();
    if (!frame->children_results.empty())
    {
        result_ast->subquery = frame->children_results[0];
    }
}

void StageQueryDistributedJoinRewriteAction::visit(const ASTSubquery * subquery_ast)
{
    auto frame = frames.getTopFrame();
    frame->result_ast = subquery_ast->clone();
    auto * result_subquery_ast = frame->result_ast->as<ASTSubquery>();
    result_subquery_ast->children = frame->children_results;
}

void StageQueryDistributedJoinRewriteAction::visit(const ASTSelectWithUnionQuery * select_with_union_ast)
{
    auto frame = frames.getTopFrame();
    auto result_union_select_ast_ref = select_with_union_ast->clone();
    auto * result_union_select_ast = result_union_select_ast_ref->as<ASTSelectWithUnionQuery>();
    result_union_select_ast->list_of_selects->children = frame->children_results;

    if (frames.size() == 1)
    {
        frame->mergeChildrenUpstreamQueries();
        frame->result_ast = ASTStageQuery::make(result_union_select_ast_ref, frame->upstream_queries[0]);
    }
    else
    {
        frame->result_ast = result_union_select_ast_ref;
    }
}

void StageQueryDistributedJoinRewriteAction::visit(const ASTSelectQuery * select_ast)
{
    auto frame = frames.getTopFrame();
    if (frame->children_results.empty())
    {
        visitSelectQueryWithAggregation(select_ast);
    }
    else if (frame->children_results.size() == 1)
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

        if (frames.size() == 1)
        {
            frame->mergeChildrenUpstreamQueries();
            frame->result_ast = ASTStageQuery::make(frame->result_ast, frame->upstream_queries[0]);
        }
    }
    else if (frame->children_results.size() == 2)
    {
        visitSelectQueryOnJoin(select_ast);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid children size.");
    }

    if (frames.size() == 1)
    {
        frame->mergeChildrenUpstreamQueries();
        frame->result_ast = ASTStageQuery::make(frame->result_ast, frame->upstream_queries[0]);
    }
}

void StageQueryDistributedJoinRewriteAction::visitSelectQueryWithAggregation(const ASTSelectQuery * select_ast)
{
    LOG_TRACE(logger, "{} visitSelectQueryWithAggregation:{}", __LINE__, queryToString(*select_ast));
    auto frame = frames.getTopFrame();
    StageQueryDistributedAggregationRewriteAction distributed_agg_action(context, id_generator);
    ASTDepthFirstVisitor<StageQueryDistributedAggregationRewriteAction> distributed_agg_visitor(distributed_agg_action, select_ast->clone());
    auto rewrite_ast = distributed_agg_visitor.visit();
    auto * stage_query = rewrite_ast->as<ASTStageQuery>();
    if (!stage_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTStageQuery is expected. return query is : {}", queryToString(rewrite_ast));
    if (!stage_query->current_query->as<ASTSelectQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTSelectQuery is expected. return query is : {}", queryToString(stage_query->current_query));

    ASTs upstream_queries;
    upstream_queries.insert(upstream_queries.end(), stage_query->upstream_queries.begin(), stage_query->upstream_queries.end());
    frame->upstream_queries = std::vector<ASTs>{upstream_queries};
    frame->result_ast = stage_query->current_query;
}

void StageQueryDistributedJoinRewriteAction::visitSelectQueryOnJoin(const ASTSelectQuery * select_ast)
{
    StageQueryDistributedJoinRewriteAnalyzer join_analyzer(select_ast, context);
    auto analyze_result = join_analyzer.analyze();
    if (!analyze_result) // Not rewrite it into shuffle pattern.
    {
        auto frame = frames.getTopFrame();
        frame->result_ast = select_ast->clone();
        auto * result_ast = frame->result_ast->as<ASTSelectQuery>();
        ASTBuildUtil::updateSelectQueryTables(
            result_ast,
            ASTBuildUtil::createTablesInSelectQueryElement(frame->children_results[0]->as<ASTTableExpression>(), nullptr)
                ->as<ASTTablesInSelectQueryElement>(),
            ASTBuildUtil::createTablesInSelectQueryElement(
                frame->children_results[1]->as<ASTTableExpression>(), result_ast->join()->table_join)
                ->as<ASTTablesInSelectQueryElement>());
    }
    else
    {
        auto frame = frames.getTopFrame();
        auto current_table_id = getNextTableId();
        auto left_shuffle_table_id = current_table_id + "_left";
        auto right_shuffle_table_id = current_table_id + "_right";
        auto * left_visited_table_expr = frame->children_results[0]->as<ASTTableExpression>();
        auto * right_visited_table_expr = frame->children_results[1]->as<ASTTableExpression>();
        ASTs upstream_queris;

        auto left_shuffle_query = createShuffleInsertForJoin(
            left_shuffle_table_id, left_visited_table_expr, analyze_result->tables_columns[0], analyze_result->tables_hash_keys[0]);
        if (frame->upstream_queries[0].empty())
            upstream_queris.emplace_back(left_shuffle_query);
        else
            upstream_queris.emplace_back(ASTStageQuery::make(left_shuffle_query, frame->upstream_queries[0]));
        auto right_shuffle_query = createShuffleInsertForJoin(
            right_shuffle_table_id, right_visited_table_expr, analyze_result->tables_columns[1], analyze_result->tables_hash_keys[1]);
        if (frame->upstream_queries[1].empty())
            upstream_queris.emplace_back(right_shuffle_query);
        else
            upstream_queris.emplace_back(ASTStageQuery::make(right_shuffle_query, frame->upstream_queries[1]));
        frame->upstream_queries = std::vector<ASTs>{upstream_queris};

        const auto & cluster_name = context->getSettings().use_cluster_for_distributed_shuffle.value;
        const auto & query_id = context->getClientInfo().current_query_id;
        frame->result_ast = select_ast->clone();
        auto * result_ast = frame->result_ast->as<ASTSelectQuery>();

        auto left_table = ASTBuildUtil::createTablesInSelectQueryElement(
            ASTBuildUtil::createShuffleTableFunction(
                TableFunctionShuffleJoin::name,
                cluster_name,
                query_id,
                left_shuffle_table_id,
                analyze_result->tables_columns[0],
                analyze_result->tables_hash_keys[0],
                ASTBuildUtil::getTableExpressionAlias(getTableExpression(*select_ast, 0)))
                ->as<ASTFunction>(),
            nullptr);
        auto right_table = ASTBuildUtil::createTablesInSelectQueryElement(
            ASTBuildUtil::createShuffleTableFunction(
                TableFunctionShuffleJoin::name,
                cluster_name,
                query_id,
                right_shuffle_table_id,
                analyze_result->tables_columns[1],
                analyze_result->tables_hash_keys[1],
                ASTBuildUtil::getTableExpressionAlias(getTableExpression(*select_ast, 1)))
                ->as<ASTFunction>(),
            select_ast->join()->table_join);
        ASTBuildUtil::updateSelectQueryTables(
            result_ast, left_table->as<ASTTablesInSelectQueryElement>(), right_table->as<ASTTablesInSelectQueryElement>());
    }
}

ASTPtr StageQueryDistributedJoinRewriteAction::createShuffleInsertForJoin(
    const String & table_id, ASTTableExpression * table_expr, const NamesAndTypesList & table_desc, const ASTPtr & hash_expr)
{
    auto select_query_ref = std::make_shared<ASTSelectQuery>();
    auto * select_query = select_query_ref->as<ASTSelectQuery>();
    ASTBuildUtil::updateSelectQueryTables(select_query, table_expr);
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, ASTBuildUtil::createSelectExpression(table_desc));

    auto hash_table = ASTBuildUtil::createShuffleTableFunction(
        TableFunctionShuffleJoin::name,
        context->getSettings().use_cluster_for_distributed_shuffle.value,
        context->getClientInfo().current_query_id,
        table_id,
        table_desc,
        hash_expr);

    return ASTBuildUtil::createTableFunctionInsertSelectQuery(hash_table, ASTBuildUtil::wrapSelectQuery(select_query));
}

StageQueryDistributedJoinRewriteAnalyzer::StageQueryDistributedJoinRewriteAnalyzer(const ASTSelectQuery * query_, ContextPtr context_)
    : from_query(query_)
    , context(context_)
    , tables_columns_from_select({{}, {}})
    , tables_columns_from_on_join({{}, {}})
    , tables_hash_keys({std::make_shared<ASTExpressionList>(), std::make_shared<ASTExpressionList>()})
    , tables_columns({{}, {}})
{
}

std::optional<StageQueryDistributedJoinRewriteAnalyzer::Result> StageQueryDistributedJoinRewriteAnalyzer::analyze()
{
    if (!isApplicableJoinType())
        return {};

    tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*from_query), context, true, true);

    if (!collectHashKeys())
        return {};

    collectTablesColumns();

    return Result{.tables_columns = tables_columns, .tables_hash_keys = tables_hash_keys};
}


bool StageQueryDistributedJoinRewriteAnalyzer::isApplicableJoinType()
{
    const auto * join_tables = from_query->join();
    auto * table_join = join_tables->table_join->as<ASTTableJoin>();

    if (table_join->kind != ASTTableJoin::Kind::Left && table_join->kind != ASTTableJoin::Kind::Inner)
        return false;
    if (table_join->strictness == ASTTableJoin::Strictness::Asof)
        return false;

    // TODO if right table is dict or special storage, return false;
    if (table_join->on_expression)
    {
        if (auto * or_func = table_join->on_expression->as<ASTFunction>(); or_func && or_func->name == "or")
        {
            LOG_INFO(logger, "Not support or join. {}", queryToString(*table_join));
            return false;
        }
    }
    else
        return false;// using clause
    
    #if 1
    // if it is a special storage, return false
    const auto * join_ast = from_query->join();
    const auto & table_to_join = join_ast->table_expression->as<ASTTableExpression &>();
    if (table_to_join.database_and_table_name)
    {
         auto joined_table_id = context->resolveStorageID(table_to_join.database_and_table_name);
        StoragePtr storage = DatabaseCatalog::instance().tryGetTable(joined_table_id, context);
        if (std::dynamic_pointer_cast<StorageDictionary>(storage))
        {
            return false;
        }
    }
    #endif

    return true;
}

bool StageQueryDistributedJoinRewriteAnalyzer::collectHashKeys()
{
    CollectAliasColumnElementAction collect_alias_cols_action;
    ASTDepthFirstVisitor<CollectAliasColumnElementAction> collect_alias_cols_visitor(collect_alias_cols_action, from_query->clone());
    auto alias_cols = collect_alias_cols_visitor.visit();

    auto * table_join = from_query->join()->table_join->as<ASTTableJoin>();
    if (table_join->on_expression)
    {
        auto * func = table_join->on_expression->as<ASTFunction>();
        if (func->name == "equals")
        {
            return collectHashKeysOnEqual(table_join->on_expression, tables_hash_keys, alias_cols);
        }
        else if (func->name == "and")
        {
            return collectHashKeysOnAnd(table_join->on_expression, tables_hash_keys, alias_cols);
        }
        else
            LOG_TRACE(logger, "Unsupport function({}) on join clause.", func->name);
        return false;
    }
    else
    {
        LOG_INFO(logger, "Join by using clause is not support now");
        return false;
    }
    __builtin_unreachable();
}

bool StageQueryDistributedJoinRewriteAnalyzer::collectHashKeysOnEqual(
    ASTPtr ast, ASTs & keys, const std::map<String, ASTPtr> & alias_columns)
{
    auto * func = ast->as<ASTFunction>();
    auto & left_arg = func->arguments->children[0];
    auto & right_arg = func->arguments->children[1];
    ASTPtr left_key = nullptr, right_key = nullptr;
    {
        CollectRequiredColumnsAction action(tables_with_columns, alias_columns);
        ASTDepthFirstVisitor<CollectRequiredColumnsAction> visitor(action, left_arg);
        auto columns = visitor.visit().required_columns;
        if (!columns[0].empty() && columns[1].empty())
        {
            left_key = left_arg;
        }
        else if (columns[0].empty() && !columns[1].empty())
        {
            right_key = left_arg;
        }
        else
        {
            LOG_INFO(logger, "Cannot find pos for arg: {}", queryToString(left_arg));
            return false;
        }
    }
    {
        CollectRequiredColumnsAction action(tables_with_columns, alias_columns);
        ASTDepthFirstVisitor<CollectRequiredColumnsAction> visitor(action, right_arg);
        auto columns = visitor.visit().required_columns;
        if (!columns[0].empty() && columns[1].empty())
        {
            left_key = right_arg;
        }
        else if (columns[0].empty() && !columns[1].empty())
        {
            right_key = right_arg;
        }
        else
        {
            LOG_INFO(logger, "Cannot find pos for arg: {}", queryToString(right_arg));
            return false;
        }
    }
    if (!left_key || !right_key)
    {
        LOG_INFO(logger, "Collect join keys failed for {}", queryToString(ast));
        return false;
    }

    auto remove_alias = [](ASTPtr alias_ast)
    {
        if (auto * ident = alias_ast->as<ASTIdentifier>())
        {
            ident->alias = "";
        }
        else if (auto * func_ast = alias_ast->as<ASTFunction>())
        {
            func_ast->alias = "";
        }
    };

    auto replace_alias_ast = [&alias_columns, remove_alias](ASTPtr & to_replace_ast)
    {
        if (auto * ident_ast = to_replace_ast->as<ASTIdentifier>())
        {
            auto iter = alias_columns.find(ident_ast->name());
            if (iter != alias_columns.end())
            {
                to_replace_ast = iter->second;
                remove_alias(to_replace_ast);
            }
        }
    };
    replace_alias_ast(left_key);
    replace_alias_ast(right_key);

    IdentifiterQualiferRemoveAction left_key_action;
    ASTDepthFirstVisitor<IdentifiterQualiferRemoveAction> left_key_visitor(left_key_action, left_key);
    left_key = left_key_visitor.visit();
    IdentifiterQualiferRemoveAction right_key_action;
    ASTDepthFirstVisitor<IdentifiterQualiferRemoveAction> right_key_visitor(right_key_action, right_key);
    right_key = right_key_visitor.visit();
    keys[0]->children.push_back(left_key);
    keys[1]->children.push_back(right_key);
    return true;
}

bool StageQueryDistributedJoinRewriteAnalyzer::collectHashKeysOnAnd(
    ASTPtr ast, ASTs & keys_list, const std::map<String, ASTPtr> & alias_columns)
{
    auto * func = ast->as<ASTFunction>();
    for (auto & arg : func->arguments->children)
    {
        auto * arg_func = arg->as<ASTFunction>();
        if (arg_func)
        {
            if (arg_func->name == "equals")
            {
                if (!collectHashKeysOnEqual(arg, keys_list, alias_columns))
                    return false;
            }
            else if (arg_func->name == "and")
            {
                if (!collectHashKeysOnAnd(arg, keys_list, alias_columns))
                    return false;
            }
        }
    }
    return true;
}

void StageQueryDistributedJoinRewriteAnalyzer::collectTablesColumns()
{
    CollectAliasColumnElementAction collect_alias_col_action;
    ASTDepthFirstVisitor<CollectAliasColumnElementAction> collect_alias_col_visitor(collect_alias_col_action, from_query->select());
    auto alias_cols = collect_alias_col_visitor.visit();

    CollectRequiredColumnsAction collect_select_action(tables_with_columns);
    ASTDepthFirstVisitor<CollectRequiredColumnsAction> collect_select_visitor(collect_select_action, from_query->select());
    tables_columns_from_select = collect_select_visitor.visit().required_columns;

    CollectRequiredColumnsAction collect_join_action(tables_with_columns, alias_cols);
    ASTDepthFirstVisitor<CollectRequiredColumnsAction> collect_join_visitor(collect_join_action, from_query->join()->table_join);
    tables_columns_from_on_join = collect_join_visitor.visit().required_columns;

    for (size_t i = 0; i < tables_columns_from_select.size(); ++i)
    {
        auto & select_columns = tables_columns_from_select[i];
        auto & join_columns = tables_columns_from_on_join[i];
        auto & result_columns = tables_columns[i];

        std::set<String> added_columns;
        for (const auto & col : select_columns)
        {
            const auto & name = col.short_name;
            if (added_columns.count(name))
                continue;
            result_columns.emplace_back(NameAndTypePair(name, col.type));
            added_columns.insert(name);
        }

        for (const auto & col : join_columns)
        {
            const auto & name = col.short_name;
            if (added_columns.count(name))
                continue;
            result_columns.emplace_back(NameAndTypePair(name, col.type));
            added_columns.insert(name);
        }
    }
}


}
