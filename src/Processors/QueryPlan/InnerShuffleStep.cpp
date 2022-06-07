#include <memory>
#include <Processors/QueryPlan/InnerShuffleStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include "Processors/Port.h"
#include <Processors/IProcessor.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <QueryPipeline/Pipe.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

InnerShuffleStep::InnerShuffleStep(const DataStream & stream_, const std::vector<size_t> & hash_columns_)
    : hash_columns(hash_columns_)
{
    input_streams = {stream_};
    output_stream = stream_;
}
void InnerShuffleStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

QueryPipelineBuilderPtr InnerShuffleStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid pipelines size, It's expected to be 1");
    }
    auto pipeline_builder = std::move(pipelines[0]);
    size_t num_streams = pipeline_builder->getNumStreams();

    Processors shuffle_processors, collect_processors;
    auto add_shuffle_transforms = [&](OutputPortRawPtrs outports) {
        Processors inner_processors;
        Block header = pipeline_builder->getHeader();
        for (auto & outport : outports)
        {
            auto dump_hash_cols = [](const std::vector<size_t> & cols)
            {
                std::string res;
                for (auto i : cols)
                {
                    res += std::to_string(i) + ",";
                }
                return res;
            };
            LOG_TRACE(&Poco::Logger::get("InnerShuffleStep"), "num_streams={}, header={}, hash_columns={}", num_streams, header.dumpNames(), dump_hash_cols(hash_columns));
            auto transform = std::make_shared<InnerShuffleTransform>(num_streams, header, hash_columns);
            connect(*outport, transform->getInputs().front());
            inner_processors.emplace_back(transform);
        }
        shuffle_processors = inner_processors;
        return inner_processors;
    };
    pipeline_builder->transform(add_shuffle_transforms);

    auto add_collect_transform = [&](OutputPortRawPtrs outports)
    {
        if (outports.size() != num_streams * num_streams)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid OutputPortRawPtrs number");
        }
        Processors inner_processors;
        Block header = pipeline_builder->getHeader();
        for (size_t i = 0; i < num_streams; ++i)
        {
            auto transform = std::make_shared<InnerShuffleCollectTransform>(num_streams, header);
            inner_processors.emplace_back(transform);
        }
        std::vector<std::list<InputPort>::iterator> collect_input_port_it;
        for (auto & processor : inner_processors)
        {
            collect_input_port_it.emplace_back(processor->getInputs().begin());
        }
        for (auto & shuffle_processor : shuffle_processors)
        {
            auto & outputs = shuffle_processor->getOutputs();
            size_t i = 0;
            for (auto & output : outputs)
            {
                auto & inputs_it = collect_input_port_it[i];
                connect(output, *inputs_it);
                inputs_it++;
                i += 1;
            }
        }
        collect_processors = inner_processors;
        return inner_processors;
    };
    pipeline_builder->transform(add_collect_transform);

    processors.insert(processors.end(), shuffle_processors.begin(), shuffle_processors.end());
    processors.insert(processors.end(), collect_processors.begin(), collect_processors.end());
    return pipeline_builder;
}

}
