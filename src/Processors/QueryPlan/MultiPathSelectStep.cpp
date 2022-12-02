#include "MultiPathSelectStep.h"
#include <memory>
#include <type_traits>
#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MultiPathSelectStep::MultiPathSelectStep(
    const Block & output_header_,
    IPathSampleSelectorPtr path_selector_,
    std::vector<PathBuilder> path_builders_,
    size_t sample_blocks_num_)
    : header(output_header_)
    , path_selector(path_selector_)
    , path_builders(path_builders_)
    , sample_blocks_num(sample_blocks_num_)
{}

// Be carefule to connect every outport with the related inport.
QueryPipelineBuilderPtr MultiPathSelectStep::updatePipeline(
    QueryPipelineBuilders pipelines,
    const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected pipeline size = 1.");
    }

    auto & pipeline = pipelines.front();
    size_t path_num = path_builders.size();
    size_t streams_num = pipeline->getNumStreams();
    auto input_header = pipeline->getHeader();
    auto sample_share_state = std::make_shared<PathSelectState>();

    // Add divide phase here. Every outport from upstream node will be divided into
    // path_num outports.
    Processors sample_processors;
    OutputPortRawPtrs sample_transform_outputs;
    auto sample_transform_build = [&](OutputPortRawPtrs outports) {
        if (outports.size() != streams_num)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unexpected ports size. outports.size={}, streams_num={}", outports.size(), streams_num);
        }
        Processors inner_processors;
        for (auto & port : outports)
        {
            auto transform = std::make_shared<MultiPathsSelectTransform>(
                input_header, path_num, sample_share_state, path_selector, sample_blocks_num);
            connect(*port, transform->getInputs().front());
            inner_processors.push_back(transform);
            for (auto & output : transform->getOutputs())
            {
                sample_transform_outputs.emplace_back(&output);
            }
        }
        sample_processors = inner_processors;
        processors.insert(processors.end(), inner_processors.begin(), inner_processors.end());
        return inner_processors;
    };
    pipeline->transform(sample_transform_build);

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
        OutputPortRawPtrs outputs;
        Processors inner_processors;
        path_builder(streams_num, inputs, &outputs, &inner_processors);
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
        processors.insert(processors.end(), inner_processors.begin(), inner_processors.end());
        return inner_processors;
    };
    pipeline->transform(paths_pipelien_build);

    auto union_transform = [&](OutputPortRawPtrs) {
        Processors inner_processors;
        if (paths_outports.size() != streams_num * path_num)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected path outports size. {} != {} * path_num",
                paths_outports.size(),
                streams_num,
                path_num);
        }
        for (size_t i = 0; i < streams_num; ++i)
        {
            OutputPortRawPtrs stream_inputs;
            for (size_t j = 0; j < path_num; ++j)
            {
                auto offset = j * streams_num + i;
                stream_inputs.emplace_back(paths_outports[offset]);
            }
            auto transform = std::make_shared<UnionStreamsTransform>(header, path_num);
            inner_processors.emplace_back(transform);
            size_t port_idx = 0;
            for (auto input : transform->getInputs())
            {
                connect(*stream_inputs[port_idx], input);
                port_idx += 1;
            }
        }
        processors.insert(processors.end(), inner_processors.begin(), inner_processors.end());
        return inner_processors;
    };
    pipeline->transform(union_transform);
    assert(pipeline->getNumStreams() == streams_num);

    return std::move(pipeline);
}

}
