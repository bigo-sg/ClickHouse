#include <memory>
#include <ucontext.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/DistributedShuffle/StorageShuffle.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionShuffle.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
void TableFunctionLocalShuffle::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    String usage_message = fmt::format(
        "The signature of function {} is:\b"
        "- session_id, table_id, table structure descrition",
        getName());

    if (args.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    cluster_name = args[0]->as<ASTLiteral&>().value.safeGet<String>();
    session_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    table_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    table_structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();

    columns = parseColumnsListFromString(table_structure, context_);
}
ColumnsDescription TableFunctionLocalShuffle::getActualTableStructure(ContextPtr) const
{
    return columns;
}

StoragePtr TableFunctionLocalShuffle::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & /*table_name*/, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage = std::make_shared<StorageLocalShuffle>(context, ast_function, cluster_name, session_id, table_id, columns);
    return storage;
}

void TableFunctionShuffleJoin::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    String usage_message = fmt::format(
        "The signature of function {} is:\b"
        "- cluster_name, session_id, table_id, table structure descrition, [hash key expression list]",
        getName());

    if (args.size() < 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    session_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    table_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    table_structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    columns = parseColumnsListFromString(table_structure, context_);
    if (args.size() >= 5)
        table_hash_exprs = args[4]->as<ASTLiteral &>().value.safeGet<String>();

    auto settings = context_->getSettings();
    ParserExpressionList hash_expr_list_parser(true);
    if (!table_hash_exprs.empty())
        hash_expr_list_ast = parseQuery(
            hash_expr_list_parser, table_hash_exprs, "Parsing table hash keys", settings.max_query_size, settings.max_parser_depth);
}
ColumnsDescription TableFunctionShuffleJoin::getActualTableStructure(ContextPtr) const
{
    return columns;
}

StoragePtr TableFunctionShuffleJoin::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & /*table_name*/, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage = std::make_shared<StorageShuffleJoin>(context, ast_function, cluster_name, session_id, table_id, columns, hash_expr_list_ast);
    return storage;
}

void TableFunctionShuffleAggregation::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    String usage_message = fmt::format(
        "The signature of function {} is:\b"
        "- cluster_name, session_id, table_id, table structure descrition, [hash key expression list]",
        getName());

    if (args.size() < 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    session_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    table_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    table_structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    columns = parseColumnsListFromString(table_structure, context_);
    if (args.size() >= 5)
        table_hash_exprs = args[4]->as<ASTLiteral &>().value.safeGet<String>();

    auto settings = context_->getSettings();
    ParserExpressionList hash_expr_list_parser(true);
    if (!table_hash_exprs.empty())
        hash_expr_list_ast = parseQuery(
            hash_expr_list_parser, table_hash_exprs, "Parsing table hash keys", settings.max_query_size, settings.max_parser_depth);
}

ColumnsDescription TableFunctionShuffleAggregation::getActualTableStructure(ContextPtr /*context*/) const
{
    return columns;
}

StoragePtr TableFunctionShuffleAggregation::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & /*table_name*/, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage = std::make_shared<StorageShuffleAggregation>(context, ast_function, cluster_name, session_id, table_id, columns, hash_expr_list_ast);
    LOG_TRACE(&Poco::Logger::get("TableFunctionShuffleAggregation"), "create agg storage. {}", storage->getName());
    return storage;
}


void TableFunctionClosedShuffle::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    String usage_message = fmt::format(
        "The signature of function {} is:\b"
        "- cluster_name, session_id, table_id",
        getName());

    if (args.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    cluster_name = args[0]->as<ASTLiteral>()->value.safeGet<String>();
    session_id = args[1]->as<ASTLiteral>()->value.safeGet<String>();
    table_id = args[2]->as<ASTLiteral>()->value.safeGet<String>();

    String table_structure = "n UInt32";
    columns = parseColumnsListFromString(table_structure, context);
}

ColumnsDescription TableFunctionClosedShuffle::getActualTableStructure(ContextPtr) const
{
    return columns;
}

StoragePtr TableFunctionClosedShuffle::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & /*table_name*/, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage = std::make_shared<StorageShuffleClose>(context, ast_function, columns, cluster_name, session_id, table_id);
    return storage;
}

void registerTableFunctionShuffle(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionLocalShuffle>();
    factory.registerFunction<TableFunctionShuffleJoin>();
    factory.registerFunction<TableFunctionShuffleAggregation>();
    factory.registerFunction<TableFunctionClosedShuffle>();
}
}
