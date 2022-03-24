#pragma once
#include <memory>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/BlockIO.h>
#include <Poco/Logger.h>
#include "Processors/QueryPlan/BuildQueryPipelineSettings.h"
#include "Processors/QueryPlan/IQueryPlanStep.h"
#include "QueryPipeline/QueryPipelineBuilder.h"
#include "Storages/IStorage.h"
namespace DB
{
class TreeQueryStep : public IQueryPlanStep
{
public:
    using BlockIOPtr = std::shared_ptr<BlockIO>;    
    using BlockIOs = std::vector<BlockIOPtr>;
    explicit TreeQueryStep(BlockIOPtr output_block_io_, const BlockIOs & input_block_ios_);
    String getName() const override { return "TreeQueryStep"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
private:
    BlockIOPtr output_block_io;
    BlockIOs input_block_ios;
    Poco::Logger * logger = &Poco::Logger::get("TreeQueryStep");
};
}
