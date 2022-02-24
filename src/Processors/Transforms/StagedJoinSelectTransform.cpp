#include <mutex>
#include <vector>
#include <Processors/Transforms/StagedJoinSelectTransform.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Common/ErrorCodes.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

#define MLOGGER &Poco::Logger::get("StagedJoinSelectTransform")
StagedJoinInsertProcess::StagedJoinInsertProcess(std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> insert_block_ios_)
    :insert_block_ios(insert_block_ios_)
{
    LOG_TRACE(MLOGGER, "insert_block_ios size:{}. {}", insert_block_ios->size(), reinterpret_cast<UInt64>(this));
}

void StagedJoinInsertProcess::runOnce()
{
    if (unlikely(!insert_block_ios->empty() && !finished))
    {
        LOG_TRACE(MLOGGER, "enter block process. blocks {}. {}", insert_block_ios->size(), reinterpret_cast<UInt64>(this));
        std::lock_guard lock(mutex);
        if (!finished)
        {
            for (auto & block_io : *insert_block_ios)
            {
                LOG_TRACE(MLOGGER, "block type: pushing:{}, pulling:{}, completed:{}", block_io->pipeline.pushing(), block_io->pipeline.pulling(), block_io->pipeline.completed());
                if (block_io->pipeline.completed())
                {
                    CompletedPipelineExecutor executor(block_io->pipeline);

                    executor.execute();
                }
                else if (block_io->pipeline.pulling())
                {
                    PullingAsyncPipelineExecutor executor(block_io->pipeline);
                    Block res;
                    executor.pull(res);
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "A pushing pipeline is not expected.");
                }
            }
            finished = true;
        }
    }

}

StagedJoinInsertTransform::StagedJoinInsertTransform(std::shared_ptr<BlockIO> insert_block_io_)
    : ISource(Block{})
    , insert_block_io(insert_block_io_)
{

}

Chunk StagedJoinInsertTransform::generate()
{
    if (insert_finished)
        return {};

    LOG_TRACE(MLOGGER, "execute block io");
    if (insert_block_io)
    {
        LOG_TRACE(
            MLOGGER,
            "block type: pushing:{}, pulling:{}, completed:{}",
            insert_block_io->pipeline.pushing(),
            insert_block_io->pipeline.pulling(),
            insert_block_io->pipeline.completed());
        if (insert_block_io->pipeline.completed())
        {
            CompletedPipelineExecutor executor(insert_block_io->pipeline);
            executor.execute();
        }
        else if (insert_block_io->pipeline.pulling())
        {
            PullingAsyncPipelineExecutor executor(insert_block_io->pipeline);
            Block res;
            executor.pull(res);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A pushing pipeline is not expected.");
        }
    }
    insert_finished = true;
    
    return {};
}

StagedJoinSelectTransform::StagedJoinSelectTransform(const Block & header_, std::shared_ptr<StagedJoinInsertProcess> insert_process_)
    : IProcessor({header_}, {header_})
    , insert_process(insert_process_)
{

}
IProcessor::Status StagedJoinSelectTransform::prepare()
{
    static int count = 0;
    LOG_TRACE(MLOGGER, "prepare block : {}. {}", count++, reinterpret_cast<UInt64>(insert_process.get()));

    insert_process->runOnce();

    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
        {
            input.close();
        }
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
        {
            input.setNotNeeded();
        }
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(std::move(chunk));
        has_output = false;
    }
    
    if (has_input)
        return Status::Ready;

    auto & input = inputs.front();
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    
    chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void StagedJoinSelectTransform::work()
{
    if(has_input)
    {
        has_input = false;
        has_output = true;
    }
}

StagedJoinSelectTransformV2::StagedJoinSelectTransformV2(const Block & header_)
    : IProcessor({header_, Block{}}, {header_})
{

}

IProcessor::Status StagedJoinSelectTransformV2::prepare()
{
    static int count = 0;
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
        {
            input.close();
        }
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input: inputs)
        {
            input.setNotNeeded();
        }
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(std::move(chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    auto & insert_part_input = inputs.back();
    if (!insert_part_input.isFinished())
    {
        insert_part_input.setNeeded();
        if (insert_part_input.hasData())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No data is expected from the insert part");
        return Status::NeedData;
    }

    auto & input = inputs.front();
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    
    chunk = input.pull(true);
    has_input = true;
    LOG_TRACE(MLOGGER, "prepared block: {} @ {}", count++, reinterpret_cast<UInt64>(this));

    return Status::Ready;
}

void StagedJoinSelectTransformV2::work()
{
    if (has_input)
    {
        has_input = false;
        has_output = true;
    }
}
}
