#pragma once
#include <memory>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/BlockIO.h>
#include <Poco/Logger.h>
#include <Processors/Transforms/TreeQueryTransform.h>
namespace DB
{
class BlockIOPhaseStep : public IQueryPlanStep
{
public:
    explicit BlockIOPhaseStep(const QueryBlockIO & block_io_, const std::vector<QueryBlockIOs> & shuffle_block_ios_);
    String getName() const override { return "BlockIOPhaseStep"; }

    //void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
    //void describeActions(FormatSettings & settings) const override;
    //void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    Poco::Logger * logger = &Poco::Logger::get("BlockIOPhaseStep");
    QueryBlockIO select_block_io;
    std::vector<QueryBlockIOs> shuffle_block_ios;

    size_t getNextBlockIOInputsSize(size_t shuffle_block_io_index);
};  
}
