#pragma once

#include <memory>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
namespace DB
{
class ASTStagedSelectQuery;
class InterpreterStagedSelectQuery : public IInterpreterUnionOrSelectQuery
{
public:
    InterpreterStagedSelectQuery(ASTPtr query_, ContextPtr context_, const SelectQueryOptions & options_);
    BlockIO execute() override;
    void buildQueryPlan(QueryPlan & query_plan) override;
    void ignoreWithTotals() override;
private:
    std::unique_ptr<InterpreterSelectWithUnionQuery> select_interpreters;
    std::vector<std::unique_ptr<InterpreterInsertQuery>> insert_interpreters;

    void initSampleBlock();

    inline std::shared_ptr<ASTStagedSelectQuery> getSelectQuery()
    {
        auto staged_select_query = std::dynamic_pointer_cast<ASTStagedSelectQuery>(query_ptr);
        if (!staged_select_query)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTStagedSelectQuery is expected, but we get : {}", queryToString(query_ptr));
        }
        return staged_select_query;
    }
};
}
