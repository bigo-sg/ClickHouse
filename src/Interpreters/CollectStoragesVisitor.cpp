#include <memory>
#include <Interpreters/CollectStoragesVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTIdentifier.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}
void CollectStoragesMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (auto select_with_union = std::dynamic_pointer_cast<ASTSelectWithUnionQuery>(ast))
    {
        visit(select_with_union, data);
    }
    else if (auto select = std::dynamic_pointer_cast<ASTSelectQuery>(ast))
    {
        visit(select, data);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknow ast type:{}", ast->getID());
    }
}

void CollectStoragesMatcher::visit(const std::shared_ptr<ASTSelectWithUnionQuery> & ast, Data & data)
{
    for (auto & child : ast->list_of_selects->children)
    {
        visit(child, data);
    }
}

void CollectStoragesMatcher::visit(const std::shared_ptr<ASTSelectQuery> & ast, Data & data)
{
    auto table_expressions = getTableExpressions(*ast);
    for (const auto & table_expression : table_expressions)
    {
        if (table_expression->database_and_table_name)
        {
            auto table_ident = std::dynamic_pointer_cast<ASTTableIdentifier>(table_expression->database_and_table_name);
            auto table_id = data.context->resolveStorageID(table_ident->getTableId());
            auto storage = DatabaseCatalog::instance().getTable(table_id, data.context);
            if (!storage)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found table {}.{}", table_id.getDatabaseName(), table_id.getFullTableName());
            data.storages.push_back(storage);
        }
        else if(table_expression->table_function)
        {
            data.storages.push_back(data.context->getQueryContext()->executeTableFunction(table_expression->table_function));
        }
        else
        {
            visit(table_expression->subquery, data);
        }
    }
}

}
