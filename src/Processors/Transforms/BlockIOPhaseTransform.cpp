#include <memory>
#include <type_traits>
#include <Processors/Transforms/BlockIOPhaseTransform.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <base/logger_useful.h>
#include "base/logger_useful.h"
#include <Common/ErrorCodes.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
SourceBlockIOPhaseTransform::SourceBlockIOPhaseTransform(std::shared_ptr<BlockIO> block_io_)
    : ISource(block_io_->pipeline.getHeader())
    , block_io(block_io_)
    , run_finished(false)
{
    is_pulling_pipeline = block_io->pipeline.pulling();
    is_completed_pipeline = block_io->pipeline.completed();
}

Chunk SourceBlockIOPhaseTransform::generate()
{
    if (run_finished)
        return {};
    if (is_completed_pipeline)
    {
        LOG_TRACE(logger, "pipeline is in completed mode");
        CompletedPipelineExecutor executor(block_io->pipeline);
        executor.execute();
        run_finished = true;
        LOG_TRACE(logger, "complted mode finished");
        return {};
    }
    else if (is_pulling_pipeline)
    {
        if (!pulling_executor)
        {
            LOG_TRACE(logger, "pipeline is in pulling mode");
            pulling_executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io->pipeline);
        }
        Block res;
        while(pulling_executor->pull(res))
        {
            if (res.rows())
            {
                LOG_TRACE(logger, "pulling pull rows:{}", res.rows());
                return Chunk(res.getColumns(), res.rows());
            }
        }

    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid pipeline with pushing() = true");
    }
    LOG_TRACE(logger, "finished");
    run_finished = true;
    return {};
}

WaitBlockIOPhaseFinishedTransform::WaitBlockIOPhaseFinishedTransform(InputPorts input_ports)
    : IProcessor(std::move(input_ports), {Block{}})
{
    for (auto & input : inputs)
    {
        running_inputs.push_back(&input);
    }
}

IProcessor::Status WaitBlockIOPhaseFinishedTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
        {
            input.close();
        }
        LOG_TRACE(logger, "finished on output finished.");
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input: inputs)
        {
            input.setNotNeeded();
        }
        LOG_TRACE(logger, "!output.canPush()");
        return Status::PortFull;
    }
    for (auto iter = running_inputs.begin(); iter != running_inputs.end(); )
    {
        auto & input = *iter;
        if (!input->isFinished())
        {
            input->setNeeded();
            if( input->hasData())
            {
                input->pullData();
            }
            iter++;
        }
        else {
            running_inputs.erase(iter++);
        }
    }
    
    if (!running_inputs.empty())
    {
        LOG_TRACE(logger, "NeedData. running inputs:{}", running_inputs.size());
        return Status::NeedData;
    }
    for (auto & out : outputs)
    {
        out.finish();
    }
    LOG_TRACE(logger, "finished");
    return Status::Finished;
}

void WaitBlockIOPhaseFinishedTransform::work()
{
}

OutputPorts SignalBlockIOPhaseFinishedTransform::makeOutputPorts(const Block & header, size_t down_stream_size)
{
    OutputPorts ports;
    for (size_t i = 0; i < down_stream_size; ++i)
    {
        ports.emplace_back(OutputPort{header});
    }
    return ports;
}

SignalBlockIOPhaseFinishedTransform::SignalBlockIOPhaseFinishedTransform(const Block & header, size_t down_stream_size)
    : IProcessor({header}, makeOutputPorts(header, down_stream_size))
{

}

IProcessor::Status SignalBlockIOPhaseFinishedTransform::prepare()
{
#if 0
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
        for (auto & input: inputs)
        {
            input.setNotNeeded();
        }
        return Status::PortFull;
    }
#endif
    auto & input = inputs.front();
    if (!input.isFinished())
    {
        input.setNeeded();
        if (input.hasData())
            input.pullData();
        LOG_TRACE(logger, "input.isFinished()");
        return Status::NeedData;
    }

    for (auto & out : outputs)
    {
        out.finish();
    }
    LOG_TRACE(logger, "finished. outputs:{}", outputs.size());
    return Status::Finished;
}

void SignalBlockIOPhaseFinishedTransform::work()
{

}

BlockIOPhaseTransform::BlockIOPhaseTransform(std::shared_ptr<BlockIO> block_io_, const Block & input_header_)
    : IProcessor({input_header_}, {block_io_->pipeline.getHeader()})
    , block_io(block_io_)
{
    LOG_TRACE(logger, "output header:{}", block_io_->pipeline.getHeader().dumpNames());
    is_pulling_pipeline = block_io->pipeline.pulling();
    is_completed_pipeline = block_io->pipeline.completed();
}

IProcessor::Status BlockIOPhaseTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
        {
            input.close();
        }
        LOG_TRACE(logger, "{} output.isFinished()", reinterpret_cast<UInt64>(this));
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input: inputs)
        {
            input.setNotNeeded();
        }
        LOG_TRACE(logger, "{} !output.canPush()", reinterpret_cast<UInt64>(this));
        return Status::PortFull;
    }

    if (has_output)
    {
        LOG_TRACE(logger, "{}, has_output. chunk rows:{}", reinterpret_cast<UInt64>(this), chunk.getNumRows());
        output.push(std::move(chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
    {
        LOG_TRACE(logger, "{} has_input. chunk rows:", reinterpret_cast<UInt64>(this), chunk.getNumRows());
        return Status::Ready;
    }

    auto & insert_part_input = inputs.back();
    if (!insert_part_input.isFinished())
    {
        insert_part_input.setNeeded();
        if (insert_part_input.hasData())
            insert_part_input.pullData();
        LOG_TRACE(logger, "{}, !insert_part_input.isFinished()", reinterpret_cast<UInt64>(this));
        return Status::NeedData;
    }

    if (is_completed_pipeline)
    {
        LOG_TRACE(logger, "{} running in completed mode", reinterpret_cast<UInt64>(this));
        CompletedPipelineExecutor executor(block_io->pipeline);
        executor.execute();
        outputs.front().finish();
        LOG_TRACE(logger, "{}, block_io->pipeline.completed()", reinterpret_cast<UInt64>(this));
        return Status::Finished;
    }
    else if (is_pulling_pipeline)
    {
        if (!pulling_executor)
        {
            pulling_executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io->pipeline);
        }
        Block res;
        LOG_TRACE(logger, "executor {} try to pull", reinterpret_cast<UInt64>(pulling_executor.get()));
        while(pulling_executor->pull(res))
        {
            LOG_TRACE(logger, "{} {} pulling rows:{}", reinterpret_cast<UInt64>(this), reinterpret_cast<UInt64>(pulling_executor.get()), res.rows());
            if (res.rows())
            {
                has_input = true;
                chunk = Chunk(res.getColumns(), res.rows());
                return Status::Ready;
            }
        }
        #if 0
        else
        {
            LOG_TRACE(logger, "{} pulling executor finished", reinterpret_cast<UInt64>(this));
            pulling_executor->getTotalsBlock();
            pulling_executor->getExtremesBlock();
            outputs.front().finish();
            return Status::Finished;
        }
        #endif
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid pipeline with pushing() = true");
    }
    outputs.front().finish();
    return Status::Finished;
}

void BlockIOPhaseTransform::work()
{
    if (has_input)
    {
        LOG_TRACE(logger, "{} work ", reinterpret_cast<UInt64>(this));
        has_output = true;
        has_input = false;
    }

}
}
