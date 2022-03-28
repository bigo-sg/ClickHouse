#include <ucontext.h>
#include <TableFunctions/TableFunctionShuffleJoin.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/DistributedShuffleJoin/StorageShuffleJoin.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
void DistTableFunctionShuffleJoin::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());
    
    ASTs & args = args_func.at(0)->children;
    String usage_message = fmt::format(
        "The signature of function {} is:\b"
        "- session_id, table_id, table structure descrition, sinks(option arg)",
        getName());
    
    if (args.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);
    
    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    session_id = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    table_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    table_structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    if (args.size() > 3)
    {
        active_sinks = args[3]->as<ASTLiteral &>().value.safeGet<UInt64>();
        LOG_TRACE(&Poco::Logger::get("DistTableFunctionShuffleJoin"), "get active_sinks: {}", active_sinks);
    }
    else {
        active_sinks = 1;
    }

    columns = parseColumnsListFromString(table_structure, context_);
}
ColumnsDescription DistTableFunctionShuffleJoin::getActualTableStructure(ContextPtr) const
{
    return columns;
}

StoragePtr DistTableFunctionShuffleJoin::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & /*table_name*/, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage = StorageShuffleJoinPart::create(context, ast_function, session_id, table_id, columns, active_sinks);
    return storage;
}
void registerTableFunctionDistShuffleJoin(TableFunctionFactory & factory)
{
    factory.registerFunction<DistTableFunctionShuffleJoin>();
}

void TableFunctionShuffleJoin::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());
    
    ASTs & args = args_func.at(0)->children;
    String usage_message = fmt::format(
        "The signature of function {} is:\b"
        "- cluster_name, session_id, table_id, table structure descrition, hash key expression list, sinks(option arg)",
        getName());
    
    if (args.size() < 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);
    
    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    session_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    table_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    table_structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    columns = parseColumnsListFromString(table_structure, context_);
    table_hash_exprs = args[4]->as<ASTLiteral &>().value.safeGet<String>();
    if (args.size() > 5)
    {
        active_sinks = args[5]->as<ASTLiteral &>().value.safeGet<UInt64>();
        LOG_TRACE(&Poco::Logger::get("DistTableFunctionShuffleJoin"), "get active_sinks: {}", active_sinks);
    }
    else 
    {
        active_sinks = 1;
    }


    auto settings = context_->getSettings();
    ParserExpressionList hash_expr_list_parser(true);
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
    StoragePtr storage = StorageShuffleJoin::create(context, ast_function, cluster_name, session_id, table_id, columns, hash_expr_list_ast, active_sinks);
    return storage;
}

void registerTableFunctionShuffleJoin(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionShuffleJoin>();
}


}
