#pragma once

#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
namespace DB
{
class MultiPathSelectStep : public IQueryPlanStep
{
public:
    using PathBuilder = std::function<void (size_t streams_num, const OutputPortRawPtrs &, OutputPortRawPtrs *, Processors *)>;
    MultiPathSelectStep(
        const Block & output_header_,
        IPathSampleSelectorPtr path_selector_,
        std::vector<PathBuilder> path_builders_,
        size_t sample_blocks_num_ = 1);
    ~MultiPathSelectStep() override = default;

    String getName() const override { return "MultiPathSelectStep"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;
private:
    Block header;
    IPathSampleSelectorPtr path_selector;
    std::vector<PathBuilder> path_builders;
    size_t sample_blocks_num;

    Processors processors;
};
}
