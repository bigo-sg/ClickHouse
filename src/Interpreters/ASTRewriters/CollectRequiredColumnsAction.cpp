#include <memory>
#include <Interpreters/ASTRewriters/CollectRequiredColumnsAction.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <Poco/String.h>
#include <Common/ErrorCodes.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int LOGICAL_ERROR;
}

void CollectRequiredColumnsAction::beforeVisitChild(const ASTPtr & /*ast*/) {}

ASTs CollectRequiredColumnsAction::collectChildren(const ASTPtr & ast)
{
    if (!ast)
        return {};
    if (const auto * function_ast = ast->as<ASTFunction>())
    {
        if (Poco::toLower(function_ast->name) == "count")
            return {};
        return function_ast->arguments->children;
    }
    else if (const auto * select_ast = ast->as<ASTSelectQuery>())
    {
        LOG_TRACE(&Poco::Logger::get("CollectRequiredColumnsAction"), "{} match select {}", __LINE__, queryToString(ast));
        ASTs children;
        children.emplace_back(select_ast->select());
        children.emplace_back(select_ast->where());
        children.emplace_back(select_ast->groupBy());
        children.emplace_back(select_ast->orderBy());
        return children;
    }
    else if (const auto * expr_list_ast = ast->as<ASTExpressionList>())
    {
        return expr_list_ast->children;
    }
    else if (const auto * join_ast = ast->as<ASTTableJoin>())
    {
        ASTs children;
        if (join_ast->using_expression_list)
            children.emplace_back(join_ast->using_expression_list);

        if (join_ast->on_expression)
            children.emplace_back(join_ast->on_expression);
        return children;
    }
    else if (const auto * orderby_ast = ast->as<ASTOrderByElement>())
    {
        return orderby_ast->children;
    }
    else if (const auto * ident_ast = ast->as<ASTIdentifier>())
    {
        ASTs children;
        if (alias_asts.count(ident_ast->name()))
            children.emplace_back(alias_asts[ident_ast->name()]);
        return children;
    }

    LOG_TRACE(&Poco::Logger::get("CollectRequiredColumnsAction"), "{} unknow ast({}) : {}", __LINE__, ast->getID(), queryToString(ast));
    return {};
}

void CollectRequiredColumnsAction::afterVisitChild(const ASTPtr & /*ast*/) {}

void CollectRequiredColumnsAction::visit(const ASTPtr & ast)
{
    if (!ast)
        return;

    if (const auto * function_ast = ast->as<ASTFunction>())
    {
        visit(function_ast);
    }
    else if (const auto * ident_ast = ast->as<ASTIdentifier>())
    {
        visit(ident_ast);
    }
    else if (const auto * asterisk_ast = ast->as<ASTAsterisk>())
    {
        visit(asterisk_ast);
    }
    else if (const auto * qualified_asterisk_ast = ast->as<ASTQualifiedAsterisk>())
    {
        visit(qualified_asterisk_ast);
    }
}

void CollectRequiredColumnsAction::visit(const ASTFunction * function_ast)
{
    if (Poco::toLower(function_ast->name) != "count")
        return;

    if (!final_result.required_columns[0].empty())
        return;

    const auto & table = tables[0];
    for (const auto & col : table.columns)
    {
        if (added_names.count(col.name))
            continue;
        ColumnWithDetailNameAndType column_metadta
            = {.full_name = table.table.alias.empty() ? col.name : table.table.alias + "." + col.name,
               .short_name = col.name,
               .alias_name = "",
               .type = col.type};
        final_result.required_columns[0].push_back(column_metadta);
        added_names.insert(col.name);
    }
}

void CollectRequiredColumnsAction::visit(const ASTIdentifier * ident_ast)
{
    if (alias_asts.count(ident_ast->name()))
        return;
    if (auto best_pos = IdentifierSemantic::chooseTableColumnMatch(*ident_ast, tables, false))
    {
        bool found = false;
        if (*best_pos < tables.size())
        {
            for (const auto & col : tables[*best_pos].columns)
            {
                if (col.name == ident_ast->shortName())
                {
                    if (added_names.count(ident_ast->name()))
                    {
                        continue;
                    }
                    ColumnWithDetailNameAndType column_metadta = {
                        .full_name = ident_ast->name(),
                        .short_name = ident_ast->shortName(),
                        .alias_name = ident_ast->tryGetAlias(),
                        .type = col.type
                    };
                    /*
                    LOG_TRACE(
                        &Poco::Logger::get("CollectRequiredColumnsAction"),
                        "add ident @ {}, full name:{}, short name:{}, alias:{}.",
                        *best_pos,
                        ident_ast->name(),
                        ident_ast->shortName(),
                        ident_ast->tryGetAlias());
                    */
                    final_result.required_columns[*best_pos].push_back(column_metadta);
                    found = true;
                    added_names.insert(ident_ast->name());
                    if (!ident_ast->alias.empty())
                        alias_names.insert(ident_ast->alias);
                    break;
                }
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid table pos: {} for ident: {}", *best_pos, queryToString(*ident_ast));
        }
        if (!found)
        {
            LOG_TRACE(&Poco::Logger::get("CollectRequiredColumnsAction"), "Not found match column for {} - {} ", queryToString(*ident_ast), ident_ast->name());
        }
    }
    else
    {
        if (!alias_names.count(ident_ast->name()))
        {
            throw Exception(
                ErrorCodes::AMBIGUOUS_COLUMN_NAME,
                "Position of identifier {} can't be deteminated. full_name={}, short_name={}, alias={}",
                queryToString(*ident_ast),
                ident_ast->name(),
                ident_ast->shortName(),
                ident_ast->alias);
        }
    }

}

void CollectRequiredColumnsAction::visit(const ASTAsterisk * /*asterisk_ast*/)
{
    for (size_t i = 0; i < tables.size(); ++i)
    {
        const auto & table_with_cols = tables[i];
        const auto & table = table_with_cols.table;
        const auto & cols = table_with_cols.columns;
        auto & required_cols = final_result.required_columns[i];
        String qualifier;
        if (!table.alias.empty())
            qualifier = table.alias;
        else if (table.table.empty())
            qualifier = table.table;

        for (const auto & col : cols)
        {
            bool has_exists = false;
            for (const auto & added_col : required_cols)
            {
                if (added_col.short_name == col.name)
                {
                    has_exists = true;
                    break;
                }
            }
            if (has_exists)
                continue;
            ColumnWithDetailNameAndType to_add_col = {
                .full_name = qualifier.empty() ? col.name : qualifier + "." + col.name,
                .short_name = col.name,
                .type = col.type
            };
            required_cols.emplace_back(to_add_col);
        }
    }
}

void CollectRequiredColumnsAction::visit(const ASTQualifiedAsterisk * qualified_asterisk)
{
    const auto & ident = qualified_asterisk->children[0];
    DatabaseAndTableWithAlias db_and_table(ident);
    for (size_t i = 0; i < tables.size(); ++i)
    {
        const auto & table_with_cols = tables[i];
        const auto & table = table_with_cols.table;
        const auto & cols = table_with_cols.columns;
        auto & required_cols = final_result.required_columns[i];
        if (!db_and_table.satisfies(table, true))
            continue;

        String qualifier = queryToString(ident);

        for (const auto & col : cols)
        {
            bool has_exists = false;
            for (const auto & added_col : required_cols)
            {
                if (added_col.short_name == col.name)
                {
                    has_exists = true;
                    break;
                }
            }
            if (has_exists)
                continue;
            ColumnWithDetailNameAndType to_add_col = {
                .full_name = qualifier.empty() ? col.name : qualifier + "." + col.name,
                .short_name = col.name,
                .type = col.type
            };
            required_cols.emplace_back(to_add_col);
        }
    }

}

}
