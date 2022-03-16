#include <memory>
#include <ucontext.h>
#include <Interpreters/CollectJoinColumnsVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/TreeDistributedShuffleJoinRewriter.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDistributedShuffleJoinSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Storages/IStorage_fwd.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;

}
#define TABLE_FUNCTION_HASHED_CHUNK_STORAGE "HashedChunksStorage"
void TreeDistributedShuffleJoinRewriteMatcher::visit(ASTPtr & ast_, Data & data_)
{
    ASTPtr final_ast;
    if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(ast_))
    {
        final_ast = visit(select_query, data_);
    }
    else if (auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(ast_))
    {
        final_ast = visit(select_with_union_query, data_);
    }
    else
    {
        final_ast = ast_;
    }
    data_.rewritten_query = final_ast;
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriteMatcher"), "stage query:{}", queryToString(data_.rewritten_query));

}

TreeDistributedShuffleJoinRewriter::TreeDistributedShuffleJoinRewriter(std::shared_ptr<ASTSelectQuery> query_, ContextPtr context_, size_t assigned_id_)
    :WithContext(context_)
    , joined_tables(context_, *query_, false)
    , query(query_)
    , assigned_id(assigned_id_)
    , joined_elements(query->join())
{
    tables_with_columns = getDatabaseAndTablesWithColumns(getTableExpressions(*query_), context_, true, true);
    tables_columns_from_on_join = {NamesAndTypesList{}, NamesAndTypesList{}};
    tables_columns_from_select = {NamesAndTypesList{}, NamesAndTypesList{}};
    tables_columns = {NamesAndTypesList{}, NamesAndTypesList{}};
    tables_hash_keys_list = {ASTExpressionList{}.clone(), ASTExpressionList{}.clone()};
}

ASTPtr TreeDistributedShuffleJoinRewriter::rewrite()
{
    if (!checkRewritable())
        return nullptr;
    if (!collectTablesColumns())
        return nullptr;
    if (!collectHashKeysList())
        return nullptr;
    //auto stages_query = std::make_shared<ASTDistributedShuffleJoinSelectQuery>();
    String hash_table_id = getContext()->getClientInfo().current_query_id;
    auto left_table_query = createLeftTableQuery(hash_table_id + "_left");
    auto right_table_query = createRightTableQuery(hash_table_id + "_right");
    auto new_select_query = createNewJoinSelectQuery(left_table_query, right_table_query);

    return new_select_query;
}
bool TreeDistributedShuffleJoinRewriter::prepare()
{
    if (!checkRewritable())
        return false;
    if (!collectTablesColumns())
        return false;
    if (!collectHashKeysList())
        return false;
    return true;
}
bool TreeDistributedShuffleJoinRewriter::checkRewritable()
{
    auto * table_join_ast = joined_elements->table_join->as<ASTTableJoin>();
    if (table_join_ast->on_expression)
    {
        if (auto * or_func = table_join_ast->on_expression->as<ASTFunction>(); or_func && or_func->name == "or")
        {
            LOG_INFO(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "Not support or join. {}", queryToString(*table_join_ast));
            return false;
        }
    }
    else {
        LOG_INFO(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "Not support join format: {}", queryToString(*table_join_ast));
        return false;
    }
    return true;
}

bool TreeDistributedShuffleJoinRewriter::collectTablesColumns()
{
    bool is_ok = collectTablesColumnsFromSelect() && collectTablesColumnsFromOnJoin();
    if (!is_ok)
        return false;
    for (size_t i = 0; i < tables_columns_from_on_join.size(); ++i)
    {
        auto & select_columns = tables_columns_from_select[i];
        auto & join_columns = tables_columns_from_on_join[i];
        auto & columns = tables_columns[i];
        std::set<String> added_columns;
        for (const auto & col : select_columns)
        {
            if (added_columns.count(col.name))
                continue;
            columns.emplace_back(col);
            added_columns.insert(col.name);
        }
        for (const auto & col : join_columns)
        {
            if (added_columns.count(col.name))
                continue;
            columns.emplace_back(col);
            added_columns.insert(col.name);
        }
    }
    LOG_TRACE(
        &Poco::Logger::get("TreeDistributedShuffleJoinRewriter"),
        "tables columns. left: {} \n right:{}",
        tables_columns[0].toString(),
        tables_columns[1].toString());
    return true;
}

bool TreeDistributedShuffleJoinRewriter::collectTablesColumnsFromSelect()
{
    
    CollectJoinColumnsMatcher::Data select_collect_data{tables_with_columns, tables_columns_from_select};
    CollectJoinColumnsVisitor(select_collect_data).visit(query->select());
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "left select cols: {}, right select cols:{}", tables_columns_from_select[0].toString(), tables_columns_from_select[1].toString());
    return true;
}

bool TreeDistributedShuffleJoinRewriter::collectTablesColumnsFromOnJoin()
{
    auto * table_join_ast = joined_elements->table_join->as<ASTTableJoin>();
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "table join ast : {}", queryToString(*table_join_ast));
    if (table_join_ast->on_expression)
    {
        CollectJoinColumnsMatcher::Data collect_data{tables_with_columns, tables_columns_from_on_join};
        CollectJoinColumnsVisitor(collect_data).visit(table_join_ast->clone());
        LOG_TRACE(
            &Poco::Logger::get("TreeDistributedShuffleJoinRewriter"),
            "left on cols: {}, right on cols:{}",
            tables_columns_from_on_join[0].toString(),
            tables_columns_from_on_join[1].toString());
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support join format: {}", queryToString(*table_join_ast));
    }
    return true;
}

bool TreeDistributedShuffleJoinRewriter::collectHashKeysList()
{
    auto * table_join_ast = joined_elements->table_join->as<ASTTableJoin>();
    if (table_join_ast->on_expression)
    {
        auto * on_func = table_join_ast->on_expression->as<ASTFunction>();
        if (on_func->name == "equals")
        {
            if (!collectHashKeysListOnEqual(table_join_ast->on_expression, tables_hash_keys_list))
                return false;
        }
        else if (on_func->name == "and")
        {
            if (!collectHashKeysListOnAnd(table_join_ast->on_expression, tables_hash_keys_list))
                return false;
        }
        else {
            LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "Unsupported join function: {}", on_func->name);
            return false;
        }
    }
    else {
        LOG_INFO(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "Not support join format: {}", queryToString(*table_join_ast));
        return false;
    }
    LOG_TRACE(
        &Poco::Logger::get("TreeDistributedShuffleJoinRewriter"),
        "left keys:{} \nright keys:{}",
        queryToString(tables_hash_keys_list[0]),
        queryToString(tables_hash_keys_list[1]));
    return true;
}

bool TreeDistributedShuffleJoinRewriter::collectHashKeysListOnEqual(ASTPtr ast, ASTs & keys_list)
{
    auto * func = ast->as<ASTFunction>();
    auto & left_arg = func->arguments->children[0];
    auto & right_arg = func->arguments->children[1];
    ASTPtr left_key = nullptr, right_key = nullptr;
    {
        std::vector<NamesAndTypesList> columns = {NamesAndTypesList{}, NamesAndTypesList{}};
        CollectJoinColumnsMatcher::Data collect_data{tables_with_columns, columns};
        CollectJoinColumnsVisitor(collect_data).visit(left_arg);
        if (!columns[0].getNames().empty() && columns[1].getNames().empty())
        {
            left_key = left_arg;
        }
        else if (columns[0].getNames().empty() && !columns[1].getNames().empty())
        {
            right_key = left_arg;
        }
        else {
            LOG_INFO(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "Cannot find pos for arg: {}", queryToString(left_arg));
            return false;
        }
    }
    {
        std::vector<NamesAndTypesList> columns = {NamesAndTypesList{}, NamesAndTypesList{}};
        CollectJoinColumnsMatcher::Data collect_data{tables_with_columns, columns};
        CollectJoinColumnsVisitor(collect_data).visit(right_arg);
        if (!columns[0].getNames().empty() && columns[1].getNames().empty())
        {
            left_key = right_arg;
        }
        else if (columns[0].getNames().empty() && !columns[1].getNames().empty())
        {
            right_key = right_arg;
        }
        else {
            LOG_INFO(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "Cannot find pos for arg: {}", queryToString(right_arg));
            return false;
        }
    }
    if (!left_key || !right_key)
    {
        LOG_INFO(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "Invalid join condition: {}", queryToString(ast));
        return false;
    }
    keys_list[0]->children.push_back(left_key);
    keys_list[1]->children.push_back(right_key);
    return true;
}

bool TreeDistributedShuffleJoinRewriter::collectHashKeysListOnAnd(ASTPtr ast, ASTs & keys_list)
{
    auto * func = ast->as<ASTFunction>();
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "fun args:{}", queryToString(func->arguments));
    for (auto & arg : func->arguments->children)
    {
        if (!collectHashKeysListOnEqual(arg, keys_list))
            return false;
    }
    return true;
}

static String tryGetTableExpressionAlias(const ASTTableExpression * table_expr)
{
    String res;
    if (table_expr->table_function)
    {
        res = table_expr->table_function->as<ASTFunction>()->tryGetAlias();
    }
    else if (table_expr->subquery)
    {
        res = table_expr->subquery->as<ASTSubquery>()->tryGetAlias();
    }
    else if (table_expr->database_and_table_name){
        if (const auto * with_alias_ast = table_expr->database_and_table_name->as<ASTTableIdentifier>())
            res = with_alias_ast->tryGetAlias();
    }
    return res;
}

ASTPtr TreeDistributedShuffleJoinRewriter::createNewJoinSelectQuery(const Strings & left_table_id, const Strings & right_table_id)
{
    auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(query->clone());
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select_query->tables();
    auto left_table_expr = std::make_shared<ASTTableExpression>();
    left_table_expr->table_function = createHashTableExpression(left_table_id, tables_columns[0], tables_hash_keys_list[0]);
    left_table_expr->table_function->as<ASTFunction>()->setAlias(tryGetTableExpressionAlias(getTableExpression(*query, 0)));
    auto left_table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    left_table_element->children.push_back(left_table_expr);
    left_table_element->table_expression = left_table_expr;
    tables->children.push_back(left_table_element);
    
    auto right_table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    right_table_element->table_join = query->join()->table_join->clone();
    auto right_table_expr = std::make_shared<ASTTableExpression>();
    right_table_expr->table_function = createHashTableExpression(right_table_id, tables_columns[1], tables_hash_keys_list[1]);
    right_table_expr->table_function->as<ASTFunction>()->setAlias(tryGetTableExpressionAlias(getTableExpression(*query, 1)));
    right_table_element->children.push_back(right_table_expr);
    right_table_element->table_expression = right_table_expr;
    tables->children.push_back(right_table_element);

    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "original left table expression: {}, alias:{}", queryToString(*getTableExpression(*query, 0)), tryGetTableExpressionAlias(getTableExpression(*query, 0)));
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "new join select query: {}", queryToString(select_query));
    return select_query;

}

ASTPtr TreeDistributedShuffleJoinRewriter::createNewJoinSelectQuery(ASTPtr & left_query, ASTPtr & right_query)
{
    auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(query->clone());
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select_query->tables();
    auto left_table_expr = std::make_shared<ASTTableExpression>();
    left_table_expr->table_function = left_query->as<ASTInsertQuery>()->table_function->clone();
    left_table_expr->table_function->as<ASTFunction>()->setAlias(tryGetTableExpressionAlias(getTableExpression(*query, 0)));
    auto left_table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    left_table_element->children.push_back(left_table_expr);
    left_table_element->table_expression = left_table_expr;
    tables->children.push_back(left_table_element);
    
    auto right_table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    right_table_element->table_join = query->join()->table_join->clone();
    auto right_table_expr = std::make_shared<ASTTableExpression>();
    right_table_expr->table_function = right_query->as<ASTInsertQuery>()->table_function->clone();
    right_table_expr->table_function->as<ASTFunction>()->setAlias(tryGetTableExpressionAlias(getTableExpression(*query, 1)));
    right_table_element->children.push_back(right_table_expr);
    right_table_element->table_expression = right_table_expr;
    tables->children.push_back(right_table_element);

    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "new join select query: {}", queryToString(select_query));
    return select_query;
}

ASTPtr TreeDistributedShuffleJoinRewriter::createLeftTableQuery(const String & hash_table_id)
{
    auto select_query = std::make_shared<ASTSelectQuery>();

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_in_select = select_query->tables();
    auto table_expression = getTableExpression(*query, 0)->clone();
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expression);
    table_element->table_expression = table_expression;
    tables_in_select->children.push_back(table_element);

    auto select_expression = std::make_shared<ASTExpressionList>();
    for (const auto & name_and_type : tables_columns[0])
    {
        auto ident = std::make_shared<ASTIdentifier>(name_and_type.name);
        select_expression->children.push_back(ident);
    }
    // TODO: where filter push down

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expression);

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(select_query);
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->children.push_back(list_of_selects);
    select_with_union_query->list_of_selects = list_of_selects;

    auto hash_table = createHashTableExpression(hash_table_id, tables_columns[0], tables_hash_keys_list[0]);
    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->table_function = hash_table;
    insert_query->select = select_with_union_query;
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "new left insert  query: {}", queryToString(insert_query));

    return insert_query;
}

ASTPtr TreeDistributedShuffleJoinRewriter::createRightTableQuery(const String & hash_table_id)
{
    auto select_query = std::make_shared<ASTSelectQuery>();

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_in_select = select_query->tables();
    auto table_expression = getTableExpression(*query, 1)->clone();
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expression);
    table_element->table_expression = table_expression;
    tables_in_select->children.push_back(table_element);

    auto select_expression = std::make_shared<ASTExpressionList>();
    for (const auto & name_and_type : tables_columns[1])
    {
        auto ident = std::make_shared<ASTIdentifier>(name_and_type.name);
        select_expression->children.push_back(ident);
    }
    // TODO: where filter push down

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expression);

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(select_query);
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->children.push_back(list_of_selects);
    select_with_union_query->list_of_selects = list_of_selects;

    auto hash_table = createHashTableExpression(hash_table_id, tables_columns[1], tables_hash_keys_list[1]);
    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->table_function = hash_table;
    insert_query->select = select_with_union_query;
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "new right insert  query: {}", queryToString(insert_query));

    return insert_query;
}

ASTPtr TreeDistributedShuffleJoinRewriter::createHashTableExpression(const String & table_id, NamesAndTypesList & columns, ASTPtr & hash_keys)
{
    auto table_func = std::make_shared<ASTFunction>();
    table_func->name = TABLE_FUNCTION_HASHED_CHUNK_STORAGE;
    table_func->arguments = std::make_shared<ASTExpressionList>();

    Field id(table_id);
    auto hash_table_id = std::make_shared<ASTLiteral>(id);
    table_func->arguments->children.push_back(hash_table_id);

    WriteBufferFromOwnString struct_buf;
    int i = 0;
    for (const auto & name_and_type : columns)
    {
        if(i)
            struct_buf << ",";
        struct_buf << name_and_type.name << " " << name_and_type.type->getName();
        i++;
    }
    auto hash_table_structure = std::make_shared<ASTLiteral>(struct_buf.str());
    table_func->arguments->children.push_back(hash_table_structure);

    auto hash_table_key = std::make_shared<ASTLiteral>(queryToString(hash_keys));
    table_func->arguments->children.push_back(hash_table_key);
    return table_func;
}

ASTPtr TreeDistributedShuffleJoinRewriter::createHashTableExpression(const Strings & table_id, NamesAndTypesList & columns, ASTPtr & hash_keys)
{
    auto table_func = std::make_shared<ASTFunction>();
    table_func->name = TABLE_FUNCTION_HASHED_CHUNK_STORAGE;
    table_func->arguments = std::make_shared<ASTExpressionList>();

    Field cluster_name(getContext()->getSettings().distributed_shuffle_join_cluster.value);
    table_func->arguments->children.push_back(std::make_shared<ASTLiteral>(cluster_name));

    Field session_id(table_id[0]);
    table_func->arguments->children.push_back(std::make_shared<ASTLiteral>(session_id));
    Field id(table_id[1]);
    table_func->arguments->children.push_back(std::make_shared<ASTLiteral>(id));

    WriteBufferFromOwnString struct_buf;
    int i = 0;
    for (const auto & name_and_type : columns)
    {
        if(i)
            struct_buf << ",";
        struct_buf << name_and_type.name << " " << name_and_type.type->getName();
        i++;
    }
    auto hash_table_structure = std::make_shared<ASTLiteral>(struct_buf.str());
    table_func->arguments->children.push_back(hash_table_structure);

    auto hash_table_key = std::make_shared<ASTLiteral>(queryToString(hash_keys));
    table_func->arguments->children.push_back(hash_table_key);
    return table_func;
}

std::shared_ptr<ASTInsertQuery> TreeDistributedShuffleJoinRewriter::createSubJoinTable(const Strings & table_id, std::shared_ptr<ASTTableExpression> table_expression)
{
    auto select_query = std::make_shared<ASTSelectQuery>();

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_in_select = select_query->tables();
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expression);
    table_element->table_expression = table_expression;
    tables_in_select->children.push_back(table_element);

    auto select_expression = std::make_shared<ASTExpressionList>();
    for (const auto & name_and_type : tables_columns[0])
    {
        auto ident = std::make_shared<ASTIdentifier>(name_and_type.name);
        select_expression->children.push_back(ident);
    }
    // TODO: where filter push down

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expression);

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(select_query);
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->children.push_back(list_of_selects);
    select_with_union_query->list_of_selects = list_of_selects;

    auto hash_table = createHashTableExpression(table_id, tables_columns[0], tables_hash_keys_list[0]);
    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->table_function = hash_table;
    insert_query->select = select_with_union_query;
    LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriter"), "new left insert  query: {}", queryToString(insert_query));

    return insert_query;
}


ASTPtr TreeDistributedShuffleJoinRewriteMatcher::visit(std::shared_ptr<ASTSelectQuery> & query_, Data & data_)
{
    const auto * joined_element = query_->join();
    if (joined_element)
    {
        auto new_query = visitSelectWithJoin(query_, data_);
        if (new_query)
            return new_query;
        return query_;
    }
    
    auto left_table_expression = extractTableExpression(*query_, 0);
    if (left_table_expression && left_table_expression->as<ASTSelectWithUnionQuery>())
    {
        auto subquery = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(left_table_expression->clone());
        auto new_subquery = visit(subquery, data_);

        auto table_expr = std::make_shared<ASTTableExpression>();
        table_expr->subquery = std::make_shared<ASTSubquery>();
        table_expr->subquery->children.push_back(new_subquery);

        auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
        table_element->children.push_back(table_expr);
        table_element->table_expression = table_expr;

        auto new_query = std::dynamic_pointer_cast<ASTSelectQuery>(query_->clone());
        new_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto tables = new_query->tables();
        tables->children.push_back(table_element);
        return new_query;
    }

    return query_;
}


ASTPtr TreeDistributedShuffleJoinRewriteMatcher::visit(std::shared_ptr<ASTSelectWithUnionQuery> & query_, Data & data_)
{
    ASTs prev_children;
    query_->list_of_selects->children.swap(prev_children);
    auto & new_children = query_->list_of_selects->children;
    for (auto & child : prev_children)
    {
        new_children.emplace_back(visitChild(child, data_));
    }
    return query_;
}

ASTPtr TreeDistributedShuffleJoinRewriteMatcher::visitChild(ASTPtr & query_, Data & data_)
{
    ASTPtr new_query;
    if (auto select_query = std::dynamic_pointer_cast<ASTSelectQuery>(query_))
    {
        new_query = visit(select_query, data_);
    }
    else if (auto select_with_union_query = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(query_))
    {
        new_query = visit(select_with_union_query, data_);   
    }
    else
    {
        new_query = query_;
    }
    if (!new_query)
    {
        //throw Exception(ErrorCodes::LOGICAL_ERROR, "Rewrite ast failed. {}", queryToString(query_));
        LOG_TRACE(&Poco::Logger::get("TreeDistributedShuffleJoinRewriteMatcher"), "Rewrite ast failed. {}", queryToString(query_));
    }
    return new_query;

}

ASTPtr TreeDistributedShuffleJoinRewriteMatcher::visitSelectWithJoin(std::shared_ptr<ASTSelectQuery> & query_, Data & data_)
{
    TreeDistributedShuffleJoinRewriter rewriter(query_, data_.context, data_.id_count);
    if (!rewriter.prepare())
        return query_;
    
    auto new_left_table_expression = visitJoinSelectTableExpression(getTableExpression(*query_, 0), data_);
    if (!new_left_table_expression)
        return query_;
    auto new_right_table_expression = visitJoinSelectTableExpression(getTableExpression(*query_, 1), data_);
    if (!new_right_table_expression)
        return query_;

    auto left_table_id = rewriter.getHashTableId("left");
    auto right_table_id = rewriter.getHashTableId("right");
    
    auto left_insert_query = rewriter.createSubJoinTable(left_table_id, new_left_table_expression);
    auto right_insert_query = rewriter.createSubJoinTable(right_table_id, new_right_table_expression);
    auto new_query = rewriter.createNewJoinSelectQuery(left_table_id, right_table_id);

    auto shuffle_join_query = std::make_shared<ASTDistributedShuffleJoinSelectQuery>();
    shuffle_join_query->shuffle_queries.emplace_back(left_insert_query);
    shuffle_join_query->shuffle_queries.emplace_back(right_insert_query);
    shuffle_join_query->select_query = new_query;

    return shuffle_join_query;
}

std::shared_ptr<ASTTableExpression> TreeDistributedShuffleJoinRewriteMatcher::visitJoinSelectTableExpression(const ASTTableExpression * table_expression, Data & data)
{
    std::shared_ptr<ASTTableExpression> new_table_expression;
    if (table_expression->database_and_table_name || table_expression->table_function)
    {
        new_table_expression = std::dynamic_pointer_cast<ASTTableExpression>(table_expression->clone());
    }
    else if (table_expression->subquery)
    {
        data.id_count++;
        auto slect_query_with_union = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(table_expression->subquery->children[0]->clone());
        auto new_left_subquery = visit(slect_query_with_union, data);
        new_table_expression = std::make_shared<ASTTableExpression>();
        auto subquery = std::make_shared<ASTSubquery>();
        subquery->children.push_back(new_left_subquery);
        subquery->setAlias(tryGetTableExpressionAlias(table_expression));   
        new_table_expression->subquery = subquery;
    }

    return new_table_expression;

}


}
