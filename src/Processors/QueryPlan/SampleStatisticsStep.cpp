#include "SampleStatisticsStep.h"
#include <memory>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

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

SampleStatisticsStep::SampleStatisticsStep(
    const DataStream & input_stream_, std::shared_ptr<BlockStatMetadata> stat_result_, size_t sample_rows_num_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , sample_rows_num(sample_rows_num_)
{
    header = input_stream_.header;
    shared_state = std::make_shared<SampleStatisticsTransform::SharedState>();
    shared_state->stat = stat_result_;
}
    
void SampleStatisticsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);
    auto build_transform = [&](OutputPortRawPtrs outputs){
        auto [new_processors, _] = QueryPipelineBuilder::connectProcessors([&](const std::vector<Block> &){
            return std::make_shared<SampleStatisticsTransform>(header, shared_state, sample_rows_num);
        },
        outputs);
        return new_processors;
    };
    pipeline.transform(build_transform);
    processors = collector.detachProcessors();
}

void SampleStatisticsStep::updateOutputStream()
{
    // Nothing to do.
}

void SampleStatisticsStep::describePipeline(FormatSettings & settings) const
{
    if (!processors.empty())
        IQueryPlanStep::describePipeline(processors, settings);

}
}
