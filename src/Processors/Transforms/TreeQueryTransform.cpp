#include <memory>
#include <Processors/Transforms/TreeQueryTransform.h>
#include <base/logger_useful.h>
#include <Common/ErrorCodes.h>
#include "Processors/Executors/CompletedPipelineExecutor.h"
#include "Core/Block.h"
#include "Processors/IProcessor.h"
#include "Processors/Port.h"

#include <Processors/Executors/CompletedPipelineExecutor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
BlockIOSourceTransform::BlockIOSourceTransform(BlockIOPtr block_io_)
    : ISource(block_io_->pipeline.getHeader())
    , block_io(block_io_)
{
    is_pulling_pipeline = block_io->pipeline.pulling();
    is_completed_pipeline = block_io->pipeline.completed();
}

Chunk BlockIOSourceTransform::generate()
{
    if (is_completed_pipeline)
    {
        LOG_TRACE(logger, "Run in completed mode");
        CompletedPipelineExecutor executor(block_io->pipeline);
        executor.execute();
        return {};
    }
    else if (is_pulling_pipeline)
    {
        if (!pulling_executor)
        {
            LOG_TRACE(logger, "Run in pulling mode");
            pulling_executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io->pipeline);
        }
        Chunk res;
        while(pulling_executor->pull(res))
        {
            if (res)
            {
                return res;
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid pipeline mode");
    }
    return {};
}

static InputPorts headersToInputPorts(const std::vector<Block> & headers)
{
    InputPorts ports;
    for (const auto & header : headers)
    {
        ports.emplace_back(header);
    }
    return ports;
}
TreeBlockIOsConnectTransform::TreeBlockIOsConnectTransform(BlockIOPtr output_block_io_, const std::vector<Block> & input_headers_, bool need_blocked_)
    : IProcessor(headersToInputPorts(input_headers_), {output_block_io_->pipeline.getHeader()})
    , output_block_io(output_block_io_)
    , need_blocked(need_blocked_)
{
    is_pulling_pipeline = output_block_io->pipeline.pulling();
    is_completed_pipeline = output_block_io->pipeline.completed();
}

IProcessor::Status TreeBlockIOsConnectTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
        {
            input.close();
        }
        LOG_TRACE(logger, "output.isFinished()");
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
        {
            input.setNotNeeded();
        }
        LOG_TRACE(logger, "!output.canPush()");
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(std::move(chunk));
        has_output = false;
        LOG_TRACE(logger, "has_output");
        return Status::PortFull;
    }

    if (has_input)
    {
        LOG_TRACE(logger, "has_input");
        return Status::Ready;
    }

    bool all_input_finished = true;
    for (auto & input : inputs)
    {
        if (!input.isFinished())
        {
            all_input_finished = false;
            input.setNeeded();
            if (input.hasData())
                (void)input.pullData();
        }
    }

    if (need_blocked && !all_input_finished)
    {
        LOG_TRACE(logger, "need_blocked && !all_input_finished");
        return Status::NeedData;
    }

    if (is_completed_pipeline)
    {
        LOG_TRACE(logger, "Run in completed mode");
        CompletedPipelineExecutor executor(output_block_io->pipeline);
        executor.execute();
        outputs.front().finish();
        return Status::Finished;
    }
    else if (is_pulling_pipeline)
    {
        if (!pulling_executor)
        {
            LOG_TRACE(logger, "Run in pullig mode");
            pulling_executor = std::make_unique<PullingAsyncPipelineExecutor>(output_block_io->pipeline);
        }
        Chunk new_chunk;
        while(pulling_executor->pull(new_chunk))
        {
            if (new_chunk)
            {
                has_input = true;
                chunk.swap(new_chunk);
                return Status::Ready;
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid pipeline");
    }

    outputs.front().finish();
    return Status::Finished;
}

void TreeBlockIOsConnectTransform::work()
{
    if (has_input)
    {
        has_input = false;
        has_output = true;
    }
}
}
