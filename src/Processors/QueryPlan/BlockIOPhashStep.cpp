#include <memory>
#include <Processors/QueryPlan/BlockIOPhaseStep.h>
#include <Processors/Transforms/BlockIOPhaseTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <functional>
#include <vector>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include "QueryPipeline/Pipe.h"
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}
BlockIOPhaseStep::BlockIOPhaseStep(const QueryBlockIO & block_io_, const std::vector<QueryBlockIOs> & shuffle_block_ios_)
    : select_block_io(block_io_)
    , shuffle_block_ios(shuffle_block_ios_)
{
    LOG_TRACE(logger, "select block io header:{}", select_block_io.block_io->pipeline.getHeader().dumpNames());
}

QueryPipelineBuilderPtr BlockIOPhaseStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & /*settings*/)
{
    if (!pipelines.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input pipelines should be empty");
    }
    auto pipeline_ptr = std::make_unique<QueryPipelineBuilder>();
    QueryPipelineBuilder & pipeline = *pipeline_ptr;

    auto source_shuffle_transform_builder = [&](const QueryBlockIOs & block_ios, size_t output_stream_num) -> Processors
    {
        if (!output_stream_num)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "output_stream_num must not be zero");

        Pipes pipes;
        for (const auto & block_io : block_ios)
        {
            pipes.emplace_back(Pipe(Processors{std::make_shared<SourceBlockIOPhaseTransform>(block_io)}));
        }
        Pipe pipe = Pipe::unitePipes(std::move(pipes));
        pipe.resize(output_stream_num);
        return Pipe::detachProcessors(std::move(pipe));
    };

    pipeline.init(Pipe(source_shuffle_transform_builder(shuffle_block_ios[0], getNextBlockIOInputsSize(0))));

    auto normal_shuffle_transform_builder = [&](OutputPortRawPtrs outports, size_t output_stream_num, const QueryBlockIOs & block_ios)
    {
        if (outports.size() != block_ios.size())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ports numbers are mismatch. outports.size() = {}, block_ios.size() = {}", outports.size(), block_ios.size());
        }

        Pipes pipes;
        for (size_t i = 0; i < block_ios.size(); ++i)
        {
            auto & outport = outports[i];
            auto  block_io = block_ios[i];
            auto transform = std::make_shared<BlockIOPhaseTransform>(block_io, outport->getHeader());
            connect(*outport, transform->getInputs().front());
            pipes.emplace_back(Pipe(Processors{transform}));
        }
        Pipe pipe = Pipe::unitePipes(std::move(pipes));
        pipe.resize(output_stream_num);
        return Pipe::detachProcessors(std::move(pipe));

    };
    for (size_t i = 1; i < shuffle_block_ios.size(); ++i)
    {
        size_t num_streams = getNextBlockIOInputsSize(i);
        LOG_TRACE(logger, "num streams={} for {}", num_streams, i);
        pipeline.transform([&](OutputPortRawPtrs outports){ return normal_shuffle_transform_builder(outports, num_streams, shuffle_block_ios[i]); });
    }

    auto select_transform_builder = [&](OutputPortRawPtrs outports, const QueryBlockIO & block_io)
    {
        if (outports.size() != 1)
           throw Exception(ErrorCodes::LOGICAL_ERROR, "Output ports should be one");
        Processors processors;
        auto select_processor = std::make_shared<BlockIOPhaseTransform>(block_io, outports[0]->getHeader());
        connect(*outports[0], select_processor->getInputs().front());
        processors.emplace_back(select_processor);
        return processors;

    };
    pipeline.transform([&](OutputPortRawPtrs outports) -> Processors { return select_transform_builder(outports, select_block_io); });

    return pipeline_ptr;
}

size_t BlockIOPhaseStep::getNextBlockIOInputsSize(size_t shuffle_block_io_index)
{
    if (shuffle_block_io_index + 1 >= shuffle_block_ios.size())
    {
        return 1;
    }
    return shuffle_block_ios[shuffle_block_io_index + 1].size();
}
}
