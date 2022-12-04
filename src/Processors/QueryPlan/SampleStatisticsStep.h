#pragma once
#include <memory>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <Processors/Transforms/SampleStatisticsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
namespace DB
{
class SampleStatisticsStep : public ITransformingStep
{
public:
    explicit SampleStatisticsStep(
        const DataStream & input_stream_,
        std::shared_ptr<BlockStatMetadata> stat_result_,
        size_t sample_rows_num_ = 4096);
    ~SampleStatisticsStep() override = default;

    String getName() const override { return "SampleStatisticsStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;
private:
    size_t sample_rows_num;
    Block header;
    std::shared_ptr<SampleStatisticsTransform::SharedState> shared_state;
    Processors processors;

    void updateOutputStream() override;
};
}
