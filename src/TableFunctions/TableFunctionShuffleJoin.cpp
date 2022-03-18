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
        "- session_id, table_id, table structure descrition",
        getName());
    
    if (args.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);
    
    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    session_id = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    table_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    table_structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    columns = parseColumnsListFromString(table_structure, context_);
}
ColumnsDescription DistTableFunctionShuffleJoin::getActualTableStructure(ContextPtr) const
{
    return columns;
}

StoragePtr DistTableFunctionShuffleJoin::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & /*table_name*/, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage = StorageShuffleJoinPart::create(context, ast_function, session_id, table_id, columns);
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
        "- cluster_name, session_id, table_id, table structure descrition, hash key expression list",
        getName());
    
    if (args.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, usage_message);
    
    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    session_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    table_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    table_structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    columns = parseColumnsListFromString(table_structure, context_);
    table_hash_exprs = args[4]->as<ASTLiteral &>().value.safeGet<String>();

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
    StoragePtr storage = StorageShuffleJoin::create(context, ast_function, cluster_name, session_id, table_id, columns, hash_expr_list_ast);
    return storage;
}

void registerTableFunctionShuffleJoin(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionShuffleJoin>();
}


}
