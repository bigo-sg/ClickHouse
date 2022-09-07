#pragma once
#include <memory>
#include <optional>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/StorageDistributedTasksBuilder.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTStageQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
#include <Processors/Transforms/StageQueryTransform.h>

namespace DB
{
class InterpreterStageQuery : public IInterpreter
{
public:
    explicit InterpreterStageQuery(ASTPtr query_, ContextPtr context_, SelectQueryOptions options_);
    BlockIO execute() override;
private:
    ASTPtr query;
    ContextPtr context;
    SelectQueryOptions options;
    Poco::Logger * logger = &Poco::Logger::get("InterpreterStageQuery");

    QueryBlockIO buildBlockIO(ASTPtr query_);

    QueryBlockIO buildInsertBlockIO(ASTPtr insert_query);
    QueryBlockIO buildSelectBlockIO(ASTPtr select_query);
    QueryBlockIO buildTreeBlockIO(ASTPtr stage_query);

    BlockIO execute(const QueryBlockIO & output_io, const QueryBlockIOs & input_block_ios);

    std::optional<std::list<std::pair<DistributedTask, String>>> tryToMakeDistributedInsertQueries(ASTPtr from_query);
    std::optional<std::list<std::pair<DistributedTask, String>>> tryToMakeDistributedSelectQueries(ASTPtr from_query);

    std::vector<StoragePtr> getSelectStorages(ASTPtr ast);

    std::list<std::pair<DistributedTask, String>> buildSelectTasks(ASTPtr from_query);

    static ASTPtr unwrapSingleSelectQuery(const ASTPtr & ast);

    static bool couldRunParallelly(const ASTStageQuery * query);
};
}
