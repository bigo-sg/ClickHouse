#include <Interpreters/ASTRewriters/CollectRequiredColumnsVisitor.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Parsers/queryToString.h>
#include <Common/ErrorCodes.h>
#include "Parsers/ASTAsterisk.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTSelectQuery.h"
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/IdentifierSemantic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int LOGICAL_ERROR;
}

void CollectRequiredColumnsMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (!ast)
        return;
    if (auto * func_ast = ast->as<ASTFunction>())
    {
        visit(*func_ast, data);
    }
    else if (auto * ident_ast = ast->as<ASTIdentifier>())
    {
        visit(*ident_ast, data);
    }
    else if (auto * select_ast = ast->as<ASTSelectQuery>())
    {
        visit(*select_ast, data);
    }
    else if (auto * expression_list = ast->as<ASTExpressionList>())
    {
        visit(*expression_list, data);   
    }
    else if (ast->as<ASTLiteral>())
    {
        return;
    }
    else if (auto * asterisk = ast->as<ASTAsterisk>())
    {
        return;
    }
    else if (auto * join_ast = ast->as<ASTTableJoin>())
    {
        visit(*join_ast, data);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknow ast type: {}", ast->getID());
    }
}

void CollectRequiredColumnsMatcher::visit(const ASTFunction & func_ast, Data & data)
{
    for (const auto & child : func_ast.arguments->children)
    {
        visit(child, data);
    }
}

void CollectRequiredColumnsMatcher::visit(const ASTSelectQuery & select_ast, Data & data)
{
    visit(select_ast.select(), data);
    visit(select_ast.where(), data);
    visit(select_ast.groupBy(), data);
    visit(select_ast.orderBy(), data);
}

void CollectRequiredColumnsMatcher::visit(const ASTExpressionList & expression_list, Data & data)
{
    for (const auto & child : expression_list.children)
    {
        visit(child, data);
    }
}

void CollectRequiredColumnsMatcher::visit(const ASTIdentifier & ident_ast, Data & data)
{
    if (auto best_pos = IdentifierSemantic::chooseTableColumnMatch(ident_ast, data.tables, false))
    {
        LOG_TRACE(&Poco::Logger::get("CollectJoinColumnsMatcher"), "table pos: {}, cols: {}", *best_pos, data.tables[*best_pos].columns.toString());
        bool found = false;
        if (*best_pos < data.tables.size())
        {
            for (const auto & col : data.tables[*best_pos].columns)
            {
                if (col.name == ident_ast.shortName())
                {
                    if (data.added_names.count(ident_ast.name()))
                        continue;
                    ColumnWithDetailNameAndType column_metadta = {
                        .full_name = ident_ast.name(), 
                        .short_name = ident_ast.shortName(),
                        .alias_name = ident_ast.tryGetAlias(),
                        .type = col.type
                    };
                    LOG_TRACE(&Poco::Logger::get("CollectJoinColumnsMatcher"), "add required columns. pos:{}, colums:{}", *best_pos, column_metadta.toString());
                    data.required_columns[*best_pos].push_back(column_metadta);
                    found = true;
                    data.added_names.insert(ident_ast.name());
                    if (!ident_ast.alias.empty())
                        data.alias_names.insert(ident_ast.alias);
                    break;
                }
            }
        }
        else 
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid table pos: {} for ident: {}", *best_pos, queryToString(ident_ast));
        }
        if (!found)
        {
            LOG_TRACE(&Poco::Logger::get("CollectJoinColumnsMatcher"), "Not found match column for {} - {} ", queryToString(ident_ast), ident_ast.name());
        }
    }
    else
    {
        if (!data.alias_names.count(ident_ast.name()))
        {
            throw Exception(
                ErrorCodes::AMBIGUOUS_COLUMN_NAME,
                "Position of identifier {} can't be deteminated. full_name={}, short_name={}, alias={}",
                queryToString(ident_ast),
                ident_ast.name(),
                ident_ast.shortName(),
                ident_ast.alias);
        }
    }
}

void CollectRequiredColumnsMatcher::visit(const ASTTableJoin & join, Data & data)
{
    if (join.using_expression_list)
    {
        visit(join.using_expression_list, data);
    }

    if (join.on_expression)
    {
        visit(join.on_expression, data);
    }
}
}
