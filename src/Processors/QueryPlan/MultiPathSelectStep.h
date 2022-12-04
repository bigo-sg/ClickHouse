#pragma once

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
namespace DB
{
class MultiPathSelectStep : public ITransformingStep
{
public:
    using PathBuilder = std::function<void (size_t streams_num, const OutputPortRawPtrs &, OutputPortRawPtrs *, Processors *)>;
    MultiPathSelectStep(
        const DataStream & input_stream_,
        IPathSampleSelectorPtr path_selector_,
        std::vector<PathBuilder> path_builders_);
    ~MultiPathSelectStep() override = default;

    String getName() const override { return "MultiPathSelectStep"; }

    // QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;
    // void describePipeline(FormatSettings & settings) const override;

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;
private:
    IPathSampleSelectorPtr path_selector;
    std::vector<PathBuilder> path_builders;

    Processors processors;

    Poco::Logger * logger = &Poco::Logger::get("MultiPathSelectStep");

    void updateOutputStream() override;
};

class DemoPathSelector : public IPathSampleSelector
{
public:
    explicit DemoPathSelector(Int32 selected_path_)
        : selected_path(selected_path_)
    {}

    ~DemoPathSelector() override = default;

    Int32 getPath() override;

private:
    Int32 selected_path;
};

class DemoPassTransform : public IProcessor
{
public:
    explicit DemoPassTransform(const Block & header);
    ~DemoPassTransform() override = default;

    String getName() const override { return "DemoPassTransform"; }
    Status prepare() override;
    void work() override;
private:
    bool has_input = false;
    bool has_output = false;
    Chunk output_chunk;
};


void buildDemoPassPipeline(
    const Block & header, size_t streams_num, const OutputPortRawPtrs & in_ports, OutputPortRawPtrs * out_ports, Processors * processors);
}
