#pragma once
#include <Interpreters/ASTRewriters/ASTDepthFirstVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{
class CollectQueryStoragesAction : public EmptyASTDepthFirstVisitAction
{
public:
    using Result = std::vector<StoragePtr>;
    explicit CollectQueryStoragesAction(ContextPtr context_) : context(context_) {}
    ~CollectQueryStoragesAction() override = default;

    ASTs collectChildren(const ASTPtr & ast) override
    {
        if (!ast)
            return {};
        ASTs children;
        if (const auto * union_select_ast = ast->as<ASTSelectWithUnionQuery>())
        {
            children = union_select_ast->list_of_selects->children;
        }
        else if (const auto * select_ast = ast->as<ASTSelectQuery>())
        {
            if (const auto * left_table_expr = getTableExpression(*select_ast, 0))
                children.emplace_back(left_table_expr->clone());
            if (const auto * right_table_expr = getTableExpression(*select_ast, 1))
                children.emplace_back(right_table_expr->clone());
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

    void visit(const ASTPtr & ast)
    {
        if (const auto * table_expr_ast = ast->as<ASTTableExpression>())
        {
            if (table_expr_ast->database_and_table_name)
            {
                auto * table_ident = table_expr_ast->database_and_table_name->as<ASTTableIdentifier>();
                auto table_id = context->resolveStorageID(table_ident->getTableId());
                auto storage = DatabaseCatalog::instance().getTable(table_id, context);
                if (storage)
                    storages.emplace_back(storage);
            }
            else if (table_expr_ast->table_function)
            {
                storages.push_back(context->getQueryContext()->executeTableFunction(table_expr_ast->table_function));
            }
        }
    }
    Result getResult() { return storages; }
private:
    ContextPtr context;
    std::vector<StoragePtr> storages;
};
}
