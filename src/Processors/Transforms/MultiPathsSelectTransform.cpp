#include "MultiPathsSelectTransform.h"
#include <memory>
#include <mutex>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>
#include "base/types.h"
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
    input.setNeeded();
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
        LOG_INFO(logger, "select path: {}", selected_path);
        Int32 n = 0;
        for (auto & port : outputs)
        {
            if (n == selected_path)
            {
                n += 1;
                continue;
            }
            n += 1;
            port.finish();
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
    has_output = true;
}
}
