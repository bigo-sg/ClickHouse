#include <memory>
#include <Processors/QueryPlan/BlockIOPhaseStep.h>
#include <Processors/Transforms/BlockIOPhaseTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/DistributedShuffleJoinTransform.h>
#include <functional>
#include <vector>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
/*
static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}
*/
BlockIOPhaseStep::BlockIOPhaseStep(std::shared_ptr<BlockIO> select_block_io_, std::vector<std::vector<std::shared_ptr<BlockIO>>> & shuffle_block_ios_)
    : select_block_io(select_block_io_)
    , shuffle_block_ios(shuffle_block_ios_)
{
    LOG_TRACE(logger, "select block io header:{}", select_block_io->pipeline.getHeader().dumpNames());
}

QueryPipelineBuilderPtr BlockIOPhaseStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & /*settings*/)
{
    if (!pipelines.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input pipelines should be empty");
    }
    auto pipeline_ptr = std::make_unique<QueryPipelineBuilder>();
    QueryPipelineBuilder & pipeline = *pipeline_ptr;
    auto source_tranform_builder = [&](OutputPortRawPtrs ports, const std::vector<std::shared_ptr<BlockIO>> & block_ios) -> Processors
    {
        if (!ports.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output ports should be empty. size:{}", ports.size());

        Processors processors;
        Processors upstream_processors;
        for (const auto & block_io : block_ios)
        {
            upstream_processors.emplace_back(std::make_shared<SourceBlockIOPhaseTransform>(block_io));
        }
        processors.insert(processors.end(), upstream_processors.begin(), upstream_processors.end());

        InputPorts inputs;
        std::vector<OutputPort*> outputs;
        for (auto & processor : upstream_processors)
        {
            auto & processor_outputs = processor->getOutputs();
            for (auto & output : processor_outputs)
            {
                inputs.emplace_back(InputPort{output.getHeader()});
                outputs.emplace_back(&output);
            }
        }
        auto unit_transform = std::make_shared<WaitBlockIOPhaseFinishedTransform>(inputs);
        processors.push_back(unit_transform);
        auto & unit_inputs = unit_transform->getInputs();
        size_t i = 0;
        for (auto & input : unit_inputs)
        {
            connect(*outputs[i], input);
            i++;
        }
        return processors;
    };
    OutputPortRawPtrs output_ports;
    LOG_TRACE(logger, "output_ports size:{}", output_ports.size());
    Pipe source_pipe(source_tranform_builder(output_ports, shuffle_block_ios[0]));
    pipeline.init(std::move(source_pipe));
    //pipeline.transform([&](OutputPortRawPtrs ports) { return source_tranform_builder(ports, shuffle_block_ios[0]); });

    auto block_io_transform_builder = [&](OutputPortRawPtrs ports, const std::vector<std::shared_ptr<BlockIO>> & block_ios) -> Processors
    {
        if (ports.size() != 1)
           throw Exception(ErrorCodes::LOGICAL_ERROR, "Output ports should be one");
        
        Processors processors;
        Processors unit_processors;
        auto resize_transform = std::make_shared<SignalBlockIOPhaseFinishedTransform>(ports[0]->getHeader(), block_ios.size());
        processors.emplace_back(resize_transform);
        auto ouput_port_iter = resize_transform->getOutputs().begin();
        auto ouput_port_iter_end = resize_transform->getOutputs().end();

        for (auto block_io : block_ios){
            if (ouput_port_iter == ouput_port_iter_end)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Output ports form upstream is dismatch with block ios");
            auto block_io_transform = std::make_shared<BlockIOPhaseTransform>(block_io, ports[0]->getHeader());
            connect(*ouput_port_iter, block_io_transform->getInputs().front());
            ouput_port_iter++;
            unit_processors.emplace_back(block_io_transform);
        }
        processors.insert(processors.end(), unit_processors.begin(), unit_processors.end());

        InputPorts inputs;
        std::vector<OutputPort*> outputs;
        for (auto & processor : unit_processors)
        {
            auto & processor_outputs = processor->getOutputs();
            for (auto & output : processor_outputs)
            {
                inputs.emplace_back(InputPort{output.getHeader()});
                outputs.emplace_back(&output);
            } 
        }

        auto unit_transform = std::make_shared<WaitBlockIOPhaseFinishedTransform>(inputs);
        processors.emplace_back(unit_transform);
        auto & unit_inputs = unit_transform->getInputs();
        size_t i = 0;
        for (auto & input : unit_inputs)
        {
            connect(*outputs[i], input);
            i++;
        }
        return processors;
         
    };   
    for (size_t i = 1; i < shuffle_block_ios.size(); ++i)
    {
        pipeline.transform([&](OutputPortRawPtrs ports) { return block_io_transform_builder(ports, shuffle_block_ios[i]); });
    }

    auto select_block_io_transform = [&](OutputPortRawPtrs ports) -> Processors
    {
        if (ports.size() != 1)
           throw Exception(ErrorCodes::LOGICAL_ERROR, "Output ports should be one");
        Processors processors;
        auto select_processor = std::make_shared<BlockIOPhaseTransform>(select_block_io, ports[0]->getHeader());
        connect(*ports[0], select_processor->getInputs().front());
        processors.emplace_back(select_processor);
        return processors;
    };
    pipeline.transform(select_block_io_transform);
    return pipeline_ptr;
}
}
