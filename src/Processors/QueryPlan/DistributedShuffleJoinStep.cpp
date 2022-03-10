#include <memory>
#include <Processors/QueryPlan/DistributedShuffleJoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/DistributedShuffleJoinTransform.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>

namespace DB
{
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

#define MLOGGER &Poco::Logger::get("DistributedShuffleJoinStep")

DistributedShuffleJoinStep::DistributedShuffleJoinStep(const DataStream & input_stream_, std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> & block_ios_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , insert_block_ios(block_ios_)
{

}

void DistributedShuffleJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    const auto & input_streams = getInputStreams();
    for (const auto & stream : input_streams)
    {
        LOG_TRACE(MLOGGER, "stream header:{}", stream.header.dumpNames());
    }
    LOG_TRACE(MLOGGER, "pipeline builder nums of output:{}. block ios size:{}", pipeline.getNumStreams(), insert_block_ios->size());
    auto tranformers_builder = [&](OutputPortRawPtrs ports) -> Processors
    {

        Processors processors;

        Pipes insert_pipes;
        for (auto & block_io : *insert_block_ios)
        {
            insert_pipes.emplace_back(Pipe(std::make_shared<DistributedShuffleJoinShuffleTransform>(block_io)));
        }
        Pipe insert_pipe(Pipe::unitePipes(std::move(insert_pipes)));
        insert_pipe.resize(ports.size());


        size_t pos = 0;
        for (auto & port : ports)
        {
            auto transform = std::make_shared<DistributedShuffleJoinTransform>(getInputStreams()[0].header);
            connect(*port, *transform->getInputs().begin());
            connect(*insert_pipe.getOutputPort(pos), transform->getInputs().back());
            processors.emplace_back(transform);
            pos += 1;
        }
        auto insert_processors = Pipe::detachProcessors(std::move(insert_pipe));
        processors.insert(processors.end(), insert_processors.begin(), insert_processors.end());
        return processors;
    };

    pipeline.transform(tranformers_builder);

}
}
