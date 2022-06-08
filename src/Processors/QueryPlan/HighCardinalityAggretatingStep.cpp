#include <memory>
#include <Processors/QueryPlan/HighCardinalityAggretatingStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include "Processors/Transforms/HighCardinalityAggregatingTransform.h"

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}
HighCardinalityAggregatingStep::HighCardinalityAggregatingStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    bool final_,
    size_t /*max_block_size_*/,
    size_t /*aggregation_in_order_max_block_bytes_*/)
    : ITransformingStep(input_stream_, params_.getHeader(final_), getTraits())
    , params(params_)
    , final(final_)
    //, max_block_size(max_block_size_)
    //, aggregation_in_order_max_block_bytes(aggregation_in_order_max_block_bytes_)
{}

void HighCardinalityAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);

    pipeline.dropTotalsAndExtremes();
    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }
    if (pipeline.getNumStreams() > 1)
    {
        pipeline.addSimpleTransform(
            [&](const Block & header)
            {
                auto agg_params = std::make_unique<HighCardinalityAggregatingTransform::Params>(params, final);
                return std::make_shared<HighCardinalityAggregatingTransform>(header, std::move(agg_params));
            });
        aggregating = collector.detachProcessors(0);
    }
    else
    {
        auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), final);
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params);
        });

        aggregating = collector.detachProcessors(0);
    }
}

void HighCardinalityAggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void HighCardinalityAggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void HighCardinalityAggregatingStep::describePipeline(FormatSettings & settings) const
{
    if (!aggregating.empty())
        IQueryPlanStep::describePipeline(aggregating, settings);
   
}

}
