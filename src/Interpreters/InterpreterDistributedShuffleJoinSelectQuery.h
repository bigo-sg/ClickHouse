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
class ASTDistributedShuffleJoinSelectQuery;
class InterpreterDistributedShuffleJoinSelectQuery : public IInterpreterUnionOrSelectQuery
{
public:
    InterpreterDistributedShuffleJoinSelectQuery(ASTPtr query_, ContextPtr context_, const SelectQueryOptions & options_);
    BlockIO execute() override;
    void buildQueryPlan(QueryPlan & query_plan) override;
    void ignoreWithTotals() override;
private:
    std::unique_ptr<InterpreterSelectWithUnionQuery> select_interpreters;
    std::vector<std::unique_ptr<InterpreterInsertQuery>> insert_interpreters;

    void initSampleBlock();

    inline std::shared_ptr<ASTDistributedShuffleJoinSelectQuery> getSelectQuery()
    {
        auto shuffle_join_select_query = std::dynamic_pointer_cast<ASTDistributedShuffleJoinSelectQuery>(query_ptr);
        if (!shuffle_join_select_query)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTDistributedShuffleJoinSelectQuery is expected, but we get : {}", queryToString(query_ptr));
        }
        return shuffle_join_select_query;
    }
};
}
