#pragma once
#include <memory>
#include <optional>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTreeQuery.h>
#include "Parsers/IAST_fwd.h"
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Poco/Logger.h>

namespace DB
{
class InterpreterTreeQuery : public IInterpreter
{
public:
    using BlockIOPtr = std::shared_ptr<BlockIO>;
    using BlockIOs = std::vector<BlockIOPtr>;

    explicit InterpreterTreeQuery(ASTPtr query_, ContextPtr context_, SelectQueryOptions options_);
    BlockIO execute() override;
private:
    ASTPtr query;
    ContextPtr context;
    SelectQueryOptions options;
    Poco::Logger * logger = &Poco::Logger::get("InterpreterTreeQuery");

    BlockIOPtr buildBlockIO(ASTPtr query_);

    BlockIOPtr buildInsertBlockIO(std::shared_ptr<ASTInsertQuery> insert_query);
    BlockIOPtr buildSelectBlockIO(std::shared_ptr<ASTSelectWithUnionQuery> select_query);
    BlockIOPtr buildTreeBlockIO(std::shared_ptr<ASTTreeQuery> tree_query);

    BlockIO execute(BlockIOPtr output_io, BlockIOs & input_block_ios);

    std::optional<std::list<std::pair<Cluster::Address, String>>> tryToMakeDistributedInsertQueries(ASTPtr from_query);
    std::optional<std::list<std::pair<Cluster::Address, String>>> tryToMakeDistributedSelectQueries(ASTPtr from_query);
};
}
