#include <memory>
#include <Processors/QueryPlan/StageQueryStep.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/StageQueryTransform.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/queryToString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
StageQueryStep::StageQueryStep(ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_)
    : context(context_), output_block_io(output_block_io_), input_block_ios(input_block_ios_)
{
}

QueryPipelineBuilderPtr StageQueryStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & /*settings*/)
{
    if (!pipelines.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "pipelines should be empty");
    }
    auto pipeline_builder_ptr = std::make_unique<QueryPipelineBuilder>();
    auto & pipeline_builder = *pipeline_builder_ptr;

    auto input_block_io_transform = [&](const QueryBlockIOs & block_ios)
    {
        Processors processors;
        for (const auto & block_io : block_ios)
        {
            processors.emplace_back(std::make_shared<BlockIOSourceTransform>(context, block_io));
        }
        return Pipe(processors);
    };
    if (!input_block_ios.empty())
    {
        auto pipe = input_block_io_transform(input_block_ios);
        pipeline_builder.init(std::move(pipe));
        LOG_TRACE(logger, "pipeline input header: {}.", pipeline_builder.getHeader().dumpNames());
    }
    else
    {
        QueryBlockIOs source_block_ios = {output_block_io};
        auto pipe = input_block_io_transform(source_block_ios);
        pipeline_builder.init(std::move(pipe));
        return pipeline_builder_ptr;
    }

    auto output_block_io_transform = [&](OutputPortRawPtrs outports)
    {
        std::vector<Block> headers;
        for (auto & outport : outports)
        {
            LOG_TRACE(logger, "upstream output header:{}.", outport->getHeader().dumpNames());
            headers.emplace_back(outport->getHeader());
        }
        auto processor = std::make_shared<StageBlockIOsConnectTransform>(context, output_block_io, headers);
        auto & in_ports = processor->getInputs();
        size_t i = 0;
        for (auto & in_port : in_ports)
        {
            connect(*outports[i], in_port);
            i++;
        }
        return Processors{processor};
    };
    pipeline_builder.transform(output_block_io_transform);
    return pipeline_builder_ptr;
}

ParallelStageQueryStep::ParallelStageQueryStep(
    ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_)
    : context(context_), output_block_io(output_block_io_), input_block_ios(input_block_ios_)
{
}

QueryPipelineBuilderPtr
ParallelStageQueryStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & /*settings*/)
{
    if (!pipelines.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "pipelines should be empty");
    }

    auto pipeline_builder_ptr = std::make_unique<QueryPipelineBuilder>();
    auto & pipeline_builder = *pipeline_builder_ptr;

    Processors processors;
    processors.emplace_back(std::make_shared<ParallelStageBlockIOsTransform>(context, output_block_io, input_block_ios));
    pipeline_builder.init(Pipe(processors));
    return pipeline_builder_ptr;
}
}
