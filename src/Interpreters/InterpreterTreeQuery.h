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
#include <Parsers/ASTTreeQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Transforms/BlockIOPhaseTransform.h>
#include <Poco/Logger.h>

namespace DB
{
class InterpreterTreeQuery : public IInterpreter
{
public:
    explicit InterpreterTreeQuery(ASTPtr query_, ContextPtr context_, SelectQueryOptions options_);
    BlockIO execute() override;
private:
    ASTPtr query;
    ContextPtr context;
    SelectQueryOptions options;
    Poco::Logger * logger = &Poco::Logger::get("InterpreterTreeQuery");

    QueryBlockIO buildBlockIO(ASTPtr query_);

    QueryBlockIO buildInsertBlockIO(std::shared_ptr<ASTInsertQuery> insert_query);
    QueryBlockIO buildSelectBlockIO(std::shared_ptr<ASTSelectWithUnionQuery> select_query);
    QueryBlockIO buildTreeBlockIO(std::shared_ptr<ASTTreeQuery> tree_query);

    BlockIO execute(const QueryBlockIO & output_io, const QueryBlockIOs & input_block_ios);

    std::optional<std::list<std::pair<DistributedTask, String>>> tryToMakeDistributedInsertQueries(ASTPtr from_query);
    std::optional<std::list<std::pair<DistributedTask, String>>> tryToMakeDistributedSelectQueries(ASTPtr from_query);

    ASTPtr fillHashedChunksStorageSinks(ASTPtr from_query, UInt64 sinks);

    std::vector<StoragePtr> getSelectStorages(ASTPtr ast);

    std::list<std::pair<DistributedTask, String>> buildSelectTasks(ASTPtr from_query);

    static ASTPtr unwrapSingleSelectQuery(const ASTPtr & ast);
};
}
