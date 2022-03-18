#pragma once
#include <memory>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/BlockIO.h>
namespace DB
{
class BlockIOPhaseStep : public ITransformingStep
{
public:
    explicit BlockIOPhaseStep(const DataStream & input_stream, std::shared_ptr<BlockIO> block_io_, std::vector<std::vector<std::shared_ptr<BlockIO>>> & shuffle_block_ios_);
    String getName() const override { return "BlockIOPhaseStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    //void describeActions(FormatSettings & settings) const override;
    //void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    std::shared_ptr<BlockIO> select_block_io;
    std::vector<std::vector<std::shared_ptr<BlockIO>>> shuffle_block_ios;
};  
}
