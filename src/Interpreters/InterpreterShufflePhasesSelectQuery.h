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
#include <Parsers/ASTShufflePhasesSelectQuery.h>
#include <Processors/Transforms/TreeQueryTransform.h>
namespace DB
{
class InterpreterShufflePhasesSelectQuery : public IInterpreterUnionOrSelectQuery
{
public:
    InterpreterShufflePhasesSelectQuery(ASTPtr query_, ContextPtr context_, const SelectQueryOptions & options_);
    BlockIO execute() override;
    void buildQueryPlan(QueryPlan & query_plan) override;
    void ignoreWithTotals() override;

private:
    void initSampleBlock();

    std::shared_ptr<ASTShufflePhasesSelectQuery> getSelectQuery();

    QueryBlockIO buildShufflePhaseBlockIO(ASTPtr query);
    QueryBlockIO buildSelectPhaseBlockIO(ASTPtr query);
};
}
