#pragma once
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
class HighCardinalityAggregatingStep : public ITransformingStep
{
public:
    HighCardinalityAggregatingStep(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        bool final_,
        size_t max_block_size_,
        size_t aggregation_in_order_max_block_bytes_);
    String getName() const override { return "HighCardinalityAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;
private:
    Aggregator::Params params;
    bool final;
    //size_t max_block_size;
    //size_t aggregation_in_order_max_block_bytes;
    Processors aggregating;
};
}
