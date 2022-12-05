#pragma once

#include <optional>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include "QueryPipeline/Pipe.h"
namespace DB
{

class DynamicPathBuilder
{
public:
    struct PathResult
    {
        Processors processors;
        OutputPortRawPtrs output_ports;
    };
    explicit DynamicPathBuilder() = default;
    virtual ~DynamicPathBuilder() = default;
    virtual PathResult buildPath(size_t num_streams, const OutputPortRawPtrs & outports) = 0;
protected:
};
using DynamicPathBuilderPtr = std::shared_ptr<DynamicPathBuilder>;

class MultiPathSelectStep : public ITransformingStep
{
public:
    using OutputHeaderBuilder = std::function<Block(const DataStream &)>;
    using TraitsBuilder = std::function<ITransformingStep::Traits()>;
    MultiPathSelectStep(
        const DataStream & input_stream_,
        OutputHeaderBuilder output_header_builder_,
        TraitsBuilder traits_builder_,
        IPathSampleSelectorPtr path_selector_,
        std::vector<DynamicPathBuilderPtr> path_builders_,
        bool need_to_clear_totals_ = false);
    ~MultiPathSelectStep() override = default;

    String getName() const override { return "MultiPathSelectStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;
private:
    OutputHeaderBuilder output_header_builder;
    TraitsBuilder traits_builder;
    IPathSampleSelectorPtr path_selector;
    std::vector<DynamicPathBuilderPtr> path_builders;
    bool need_to_clear_totals;
    Block output_header;

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
