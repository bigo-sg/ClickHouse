#include "MultiPathsSelectTransform.h"
#include <memory>
#include <mutex>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>
#include <Processors/Port.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static OutputPorts splitOutputport(size_t num_streams, const Block & header)
{
    OutputPorts outputs;
    for (size_t i = 0; i < num_streams; ++i)
    {
        OutputPort output(header);
        outputs.push_back(output);
    }
    return outputs;
}

static InputPorts splitInputPort(size_t num_streams, const Block & header)
{
    InputPorts ports;
    for (size_t i = 0; i < num_streams; ++i)
    {
        InputPort port(header);
        ports.push_back(port);
    }
    return ports;
}

MultiPathsSelectTransform::MultiPathsSelectTransform(const Block & header_,
        size_t path_num_,
        IPathSampleSelectorPtr path_selector_)
    : IProcessor({header_}, splitOutputport(path_num_, header_))
    , header(header_)
    , path_selector(path_selector_)
{
    for (auto & port : outputs)
    {
        outputs_ptrs.emplace_back(&port);
    }
}

IProcessor::Status MultiPathsSelectTransform::prepare()
{
    auto * output = outputs_ptrs[selected_path < 0 ? 0 : selected_path];
    auto & input = inputs.front();

    if (output->isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output->canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_output)
    {
        output->push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    if (input.isFinished())
    {
        for (auto & port : outputs)
        {
            port.finish();
        }
        return Status::Finished;
    }

    if (!input.hasData())
    {
        return Status::NeedData;        
    }

    if (selected_path < 0) [[unlikely]]
    {
        selected_path = path_selector->getPath();
        if (selected_path < 0)
        {
            // do nothing.
            return Status::Ready;
        }
    }

    output_chunk = input.pull(true);
    has_input = true;

    return Status::Ready;
}

void MultiPathsSelectTransform::work()
{
    if (selected_path < 0) [[unlikely]]
    {
        LOG_DEBUG(logger, "No path selected");
        return;
    }

    if (!has_input)[[unlikely]]
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected has_input = true");
    }

    has_input = false;
    has_output = false;
}

UnionStreamsTransform::UnionStreamsTransform(const Block & header_, size_t inputs_num)
    : IProcessor(splitInputPort(inputs_num, header_), {header_})
{
    for (auto & port : inputs)
    {
        running_inputs.emplace_back(&port);
    }
}

IProcessor::Status UnionStreamsTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto * input : running_inputs)
        {
            if (!input->isFinished())
                input->close();
        }
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto * input : running_inputs)
        {
            if (!input->isFinished())
            {
                input->setNotNeeded();
            }
        }
        return Status::PortFull;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    bool all_inputs_closed = true;
    for (auto it = running_inputs.begin(); it != running_inputs.end();)
    {
        auto * input = *it;
        if (input->isFinished())
        {
            running_inputs.erase(it++);
            continue;
        }
        it++;
        all_inputs_closed = false;
        input->setNeeded();
        if (!input->hasData())
        {
            continue;
        }
        output_chunk = input->pull(true);
        has_input = true;
        break;
    }

    if (all_inputs_closed)
    {
        for (auto & port : outputs)
        {
            if (!port.isFinished())
            {
                port.finish();
            }
        }
        return Status::Finished;
    }

    if (!has_input)
        return Status::NeedData;
    return Status::Ready;
}

void UnionStreamsTransform::work()
{
    if (has_input)
    {
        has_input = false;
        has_output = true;
    }
}
}
