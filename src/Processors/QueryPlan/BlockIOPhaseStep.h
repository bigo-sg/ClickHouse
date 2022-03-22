#pragma once
#include <memory>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/BlockIO.h>
#include <Poco/Logger.h>
namespace DB
{
class BlockIOPhaseStep : public IQueryPlanStep
{
public:
    explicit BlockIOPhaseStep(std::shared_ptr<BlockIO> block_io_, std::vector<std::vector<std::shared_ptr<BlockIO>>> & shuffle_block_ios_);
    String getName() const override { return "BlockIOPhaseStep"; }

    //void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
    //void describeActions(FormatSettings & settings) const override;
    //void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    Poco::Logger * logger = &Poco::Logger::get("BlockIOPhaseStep");
    std::shared_ptr<BlockIO> select_block_io;
    std::vector<std::vector<std::shared_ptr<BlockIO>>> shuffle_block_ios;

    size_t getNextBlockIOInputsSize(size_t shuffle_block_io_index);
};  
}
