#include <memory>
#include <Processors/QueryPlan/TreeQueryStep.h>
#include <base/logger_useful.h>
#include "Common/Exception.h"
#include <Common/ErrorCodes.h>
#include "Processors/IProcessor.h"
#include "Processors/Transforms/TreeQueryTransform.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
TreeQueryStep::TreeQueryStep(BlockIOPtr output_block_io_, const BlockIOs & input_block_ios_)
    : output_block_io(output_block_io_)
    , input_block_ios(input_block_ios_)
{

}

QueryPipelineBuilderPtr TreeQueryStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & /*settings*/)
{
    if (!pipelines.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "pipelines should be empty");
    }
    auto pipeline_builder_ptr = std::make_unique<QueryPipelineBuilder>();
    auto & pipeline_builder = *pipeline_builder_ptr;

    auto input_block_io_transform = [&](BlockIOs block_ios)
    {
        Processors processors;
        for (const auto & block_io : block_ios)
        {
            processors.emplace_back(std::make_shared<BlockIOSourceTransform>(block_io));
        }
        return Pipe(processors);
    };
    auto pipe = input_block_io_transform(input_block_ios);
    pipeline_builder.init(std::move(pipe));
    LOG_TRACE(logger, "pipeline input header: {}.", pipeline_builder.getHeader().dumpNames());

    auto output_block_io_transform =
        [&](OutputPortRawPtrs outports)
    {
        std::vector<Block> headers;
        for (auto & outport : outports)
        {
            LOG_TRACE(logger, "upstream output header:{}.", outport->getHeader().dumpNames());
            headers.emplace_back(outport->getHeader());
        }
        auto processor = std::make_shared<TreeBlockIOsConnectTransform>(output_block_io, headers, true);
        auto & inports = processor->getInputs();
        size_t i = 0;
        for (auto & inport : inports)
        {
            connect(*outports[i], inport);
            i++;
        }
        LOG_TRACE(logger, "upstream outports: {}, current node inputports: {}", outports.size(), processor->getInputs().size());
        LOG_TRACE(logger, "current node output header:{}", processor->getOutputs().front().getHeader().dumpNames());
        return Processors{processor};
    };

    pipeline_builder.transform(output_block_io_transform);
    return pipeline_builder_ptr;
}
}
