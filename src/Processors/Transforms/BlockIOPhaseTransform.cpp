#include <memory>
#include <type_traits>
#include <Processors/Transforms/BlockIOPhaseTransform.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

namespace DB
{
SourceBlockIOPhaseTransform::SourceBlockIOPhaseTransform(std::shared_ptr<BlockIO> block_io_)
    : ISource(block_io_->pipeline.getHeader())
    , block_io(block_io_)
    , run_finished(false)
{
}

Chunk SourceBlockIOPhaseTransform::generate()
{
    if (run_finished)
        return {};
    if (block_io->pipeline.completed())
    {
        CompletedPipelineExecutor executor(block_io->pipeline);
        executor.execute();
        run_finished = true;
        return {};
    }
    else if (block_io->pipeline.pulling())
    {
        if (!pulling_exetuor)
        {
            pulling_exetuor = std::make_unique<PullingAsyncPipelineExecutor>(block_io->pipeline);
        }
        Block res;
        pulling_exetuor->pull(res);
        if (res.rows())
        {
            run_finished = true;
            return Chunk(res.getColumns(), res.rows());
        }

    }
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
        return Status::NeedData;
    }
    for (auto & out : outputs)
    {
        out.finish();
    }
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

    auto & input = inputs.front();
    if (!input.isFinished())
    {
        input.setNeeded();
        if (input.hasData())
            input.pullData();
        return Status::NeedData;
    }

    for (auto & out : outputs)
    {
        out.finish();
    }
    return Status::Finished;
}

void SignalBlockIOPhaseFinishedTransform::work()
{

}

BlockIOPhaseTransform::BlockIOPhaseTransform(std::shared_ptr<BlockIO> block_io_, const Block & input_header_)
    : IProcessor({input_header_}, {block_io_->pipeline.getHeader()})
    , block_io(block_io_)
{

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
            insert_part_input.pullData();
        return Status::NeedData;
    }

    if (block_io->pipeline.completed())
    {
        CompletedPipelineExecutor executor(block_io->pipeline);
        executor.execute();
        outputs.front().finish();
    }
    else if (block_io->pipeline.pulling())
    {
        if (!pulling_exetuor)
        {
            pulling_exetuor = std::make_unique<PullingAsyncPipelineExecutor>(block_io->pipeline);
        }
        Block res;
        pulling_exetuor->pull(res);
        if (res.rows())
        {
            chunk = Chunk(res.getColumns(), res.rows());
        }
        else
        {
            outputs.front().finish();
        }
    }
    has_input = true;
    return Status::Ready;
}

void BlockIOPhaseTransform::work()
{
    if (has_input)
    {
        has_output = true;
        has_input = false;
    }

}
}
