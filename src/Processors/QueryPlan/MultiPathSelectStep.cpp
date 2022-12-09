#include "MultiPathSelectStep.h"
#include <memory>
#include <type_traits>
#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/HighCardinalityAggregatingTransform.h>
#include <Analyzer/BlockStatAnalyzer.h>
#include <Processors/IProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MultiPathSelectStep::MultiPathSelectStep(
    const DataStream & input_stream_,
    OutputHeaderBuilder output_header_builder_,
    TraitsBuilder traits_builder_,
    IPathSampleSelectorPtr path_selector_,
    std::vector<DynamicPathBuilderPtr> path_builders_,
    bool need_to_clear_totals_)
    : ITransformingStep(input_stream_, output_header_builder_(input_stream_), traits_builder_())
    , output_header_builder(output_header_builder_)
    , traits_builder(traits_builder_)
    , path_selector(path_selector_)
    , path_builders(path_builders_)
    , need_to_clear_totals(need_to_clear_totals_)
{
    output_header = output_header_builder(input_stream_);
}

// Be carefule to connect every outport with the related inport.
void MultiPathSelectStep::transformPipeline(
    QueryPipelineBuilder & pipeline,
    const BuildQueryPipelineSettings &)
{
    LOG_DEBUG(logger, "line:{}, streams:{}", __LINE__, pipeline.getNumStreams());
    if (need_to_clear_totals)
        pipeline.dropTotalsAndExtremes();

    QueryPipelineProcessorsCollector collector(pipeline, this);
    size_t path_num = path_builders.size();
    size_t streams_num = pipeline.getNumStreams();
    path_selector->setNumStreams(streams_num);
    auto input_header = pipeline.getHeader();

    // Add divide phase here. Every outport from upstream node will be divided into
    // path_num outports. Every new outport is related to one execution path.
    Processors sample_processors;
    OutputPortRawPtrs sample_transform_outputs;
    
    auto sample_transform_build = [&](OutputPortRawPtrs outports)
    {
        auto build_multi_paths_select_transform = [&](const std::vector<Block> & /*headers*/)
        { return std::make_shared<MultiPathsSelectTransform>(input_header, path_num, path_selector); };
        auto result = QueryPipelineBuilder::connectProcessors(build_multi_paths_select_transform, outports);
        sample_processors.swap(result.first);
        sample_transform_outputs.swap(result.second);
        return sample_processors;
    };
    pipeline.transform(sample_transform_build);

    if (sample_transform_outputs.size() != path_num * streams_num)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unexpected outports size. {} != {} * {}", sample_transform_outputs.size(), path_num, streams_num);
    }
    Processors path_pipeline_processors;
    OutputPortRawPtrs paths_outports;
    auto path_pipeline_build = [&](size_t index) {
        auto & path_builder = path_builders[index];
        // collect related outports from upstream
        OutputPortRawPtrs inputs;
        for (size_t i = 0; i < streams_num; ++i)
        {
            auto offset = i * path_num + index;
            inputs.emplace_back(sample_transform_outputs[offset]);
        }
        auto [inner_processors, outputs] = path_builder->buildPath(streams_num, inputs);
        path_pipeline_processors.insert(path_pipeline_processors.end(), inner_processors.begin(), inner_processors.end());
        paths_outports.insert(paths_outports.end(), outputs.begin(), outputs.end());
        return inner_processors;
    };
    auto paths_pipelien_build = [&](OutputPortRawPtrs)
    {
        Processors inner_processors;
        for (size_t i = 0; i < path_builders.size(); ++i)
        {
            auto procs = path_pipeline_build(i);
            inner_processors.insert(inner_processors.end(), procs.begin(), procs.end());
        }
        return inner_processors;
    };
    pipeline.transform(paths_pipelien_build);

    for (auto & paths_outport : paths_outports)
    {
        auto header = paths_outport->getHeader();
        if (!blocksHaveEqualStructure(output_header, header))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Ports has different structure.({}) vs. ({})", output_header.dumpNames(), header.dumpNames());
        }
    }

    auto union_transform = [&](OutputPortRawPtrs) {
        if (paths_outports.size() != streams_num * path_num)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected path outports size. {} != {} * {}",
                paths_outports.size(),
                streams_num,
                path_num);
        }

        OutputPortRawPtrs rearranged_ports;
        for (size_t i = 0; i < streams_num; ++i)
        {
            for (size_t j = 0; j < path_num; ++j)
            {
                auto offset = j * streams_num + i;
                rearranged_ports.emplace_back(paths_outports[offset]);
            }
        }
        auto [new_processors, new_outputs] = QueryPipelineBuilder::connectProcessors(
            [&](const std::vector<Block> & blocks) { return std::make_shared<UnionStreamsTransform>(blocks[0], path_num); },
            rearranged_ports,
            path_num);
        return new_processors;

    };
    pipeline.transform(union_transform);
    assert(pipeline.getNumStreams() == streams_num);

    processors = collector.detachProcessors(0);

}

void MultiPathSelectStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), output_header_builder(input_streams.front()), traits_builder().data_stream_traits);
    // Nothing to do.
}

void MultiPathSelectStep::describePipeline(FormatSettings & settings) const
{
    if (!processors.empty())
        IQueryPlanStep::describePipeline(processors, settings);

}

Int32 DemoPathSelector::getPath()
{
    return selected_path;
}

DemoPassTransform::DemoPassTransform(const Block & header)
    : IProcessor({header}, {header})
{}

IProcessor::Status DemoPassTransform::prepare()
{
    auto & output = getOutputs().front();
    auto & input = getInputs().front();
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
    {
        return Status::NeedData;
    }
    output_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void DemoPassTransform::work()
{
    if (has_input)
    {
        has_input = false;
        has_output = true;
    }
}

void buildDemoPassPipeline(
    const Block & header, size_t streams_num, const OutputPortRawPtrs & in_ports, OutputPortRawPtrs * out_ports, Processors * processors)
{
    assert(streams_num == in_ports.size());
    for (auto * in_port : in_ports)
    {
        auto transform = std::make_shared<DemoPassTransform>(header);
        processors->emplace_back(transform);
        connect(*in_port, transform->getInputs().front());
        out_ports->emplace_back(&transform->getOutputs().front());
    }
}

}
