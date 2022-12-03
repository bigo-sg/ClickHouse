#include "MultiPathSelectStep.h"
#include <memory>
#include <type_traits>
#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include "Analyzer/BlockStatAnalyzer.h"
#include <Processors/IProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
MultiPathSelectStep::MultiPathSelectStep(
    const DataStream & input_stream_,
    IPathSampleSelectorPtr path_selector_,
    std::vector<PathBuilder> path_builders_,
    size_t sample_blocks_num_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , path_selector(path_selector_)
    , path_builders(path_builders_)
    , sample_blocks_num(sample_blocks_num_)
{}

// Be carefule to connect every outport with the related inport.
void MultiPathSelectStep::transformPipeline(
    QueryPipelineBuilder & pipeline,
    const BuildQueryPipelineSettings &)
{
    LOG_ERROR(logger, "{} {} {}", __LINE__, reinterpret_cast<UInt64>(this), pipeline.getNumStreams());
    QueryPipelineProcessorsCollector collector(pipeline, this);
    size_t path_num = path_builders.size();
    size_t streams_num = pipeline.getNumStreams();
    auto input_header = pipeline.getHeader();
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
        return inner_processors;
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
        return inner_processors;
    };
    pipeline.transform(paths_pipelien_build);

    for (size_t i = 1; i < paths_outports.size(); ++i)
    {
        auto lheader = paths_outports[i-1]->getHeader();
        auto rheader = paths_outports[i]->getHeader();
        if (!blocksHaveEqualStructure(lheader, rheader))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Ports has different structure.({}) vs. ({})", lheader.dumpNames(), rheader.dumpNames());
        }
    }

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
            auto transform = std::make_shared<UnionStreamsTransform>(paths_outports[0]->getHeader(), path_num);
            inner_processors.emplace_back(transform);
            size_t port_idx = 0;
            if (transform->getInputs().size() != stream_inputs.size())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid size: {} vs. {}", transform->getInputs().size(), stream_inputs.size());
            }
            for (auto & input : transform->getInputs())
            {
                connect(*stream_inputs[port_idx], input);
                port_idx += 1;
            }
        }
        return inner_processors;
    };
    pipeline.transform(union_transform);
    assert(pipeline.getNumStreams() == streams_num);

    output_stream = createOutputStream(getInputStreams().front(), paths_outports[0]->getHeader(), getTraits().data_stream_traits);
    processors = collector.detachProcessors(0);
}

void MultiPathSelectStep::updateOutputStream()
{
    LOG_ERROR(logger, "xxxx call updateOutputStream");
    // Nothing to do.
}

void MultiPathSelectStep::describePipeline(FormatSettings & settings) const
{
    if (!processors.empty())
        IQueryPlanStep::describePipeline(processors, settings);

}

Int32 DemoPathSelector::compute(const std::list<Chunk> & samples)
{
    std::vector<Block> blocks;
    for (const auto & chunk : samples)
    {
        auto cols = chunk.getColumns();
        ColumnsWithTypeAndName cols_with_name_type;
        for (size_t i = 0; i < cols.size(); ++i)
        {
            auto & block_col = header.getByPosition(i);
            ColumnWithTypeAndName col_with_name_type(cols[i],  block_col.type, block_col.name);
            cols_with_name_type.emplace_back(col_with_name_type);
        }
        blocks.emplace_back(cols_with_name_type);
    }
    BlockStatAnalyzer analyzer(blocks);
    auto result = analyzer.analyze();
    for (auto & data : result.columns_metadata)
    {
        LOG_ERROR(&Poco::Logger::get("DemoPathSelector"), "xxxx stat: {}", data->debugString());
    }
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
