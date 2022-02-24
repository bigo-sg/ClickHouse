#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/BlockIO.h>
namespace DB
{
class StagedJoinSelectStep : public ITransformingStep
{
public:
    explicit StagedJoinSelectStep(const DataStream & input_stream_, std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> & block_ios_);
    String getName() const override { return "StagedJoinSelect"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    //void describeActions(FormatSettings & settings) const override;
    //void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> insert_block_ios;
};
}
