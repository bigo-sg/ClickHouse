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

PathSelectState::Status PathSelectState::getStatus()
{
    return status;
}

void PathSelectState::setStatus(Status status_)
{
    status = status_;
}

Int32 PathSelectState::getPath()
{
    return selected_path;
}

void PathSelectState::setPath(Int32 path)
{
    selected_path = path;
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
        PathSelectStatePtr shared_state_,
        IPathSampleSelectorPtr path_selector_,
        size_t sample_rows_num_)
    : IProcessor({header_}, splitOutputport(path_num_, header_))
    , header(header_)
    , path_num(path_num_)
    , shared_state(shared_state_)
    , path_selector(path_selector_)
    , sample_rows_num(sample_rows_num_)
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

    if (local_status != PathSelectState::AFTER_SAMPLE && selected_path < 0) [[unlikely]]
    {
        return samplingPrepare(input);
    }
    return normalPrepare(input);
}

IProcessor::Status MultiPathsSelectTransform::normalPrepare(InputPort & input)
{
    if (has_input)
    {
        return Status::Ready;
    }

    if (input.isFinished())
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
    input.setNeeded();
    if (!input.hasData())
    {
        return Status::NeedData;
    }
    output_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

IProcessor::Status MultiPathsSelectTransform::samplingPrepare(InputPort & input)
{
    if (local_status < PathSelectState::SAMPLING)
    {
        // Every processor enters this part once.
        std::lock_guard lock(shared_state->getSamplingMutex());
        auto current_status = shared_state->getStatus();
        if (current_status == PathSelectState::BEFORE_SAMPLE)
        {
            // only one processor do the sample. other processors will wait for this
            // processor finishing sampling.
            local_status = PathSelectState::SAMPLING;
            shared_state->setStatus(local_status);
        }
        else if (current_status == PathSelectState::SAMPLING)
        {
            // other processors that do not sample, just wait the previous one finished.
            local_status = PathSelectState::WAIT_SAMPLE_FINISHED;
        }
        else
        {
            // has finished sample.
            local_status = current_status;
        }
    }

    if (local_status == PathSelectState::WAIT_SAMPLE_FINISHED)
    {
        auto current_status = shared_state->getStatus();
        if (current_status == PathSelectState::SAMPLING)
        {
            // let it run idlly
            return Status::Ready;
        }
        // the status is expected to be AFTER_SAMPLE here.        
        local_status = current_status;
    }

    if (selected_path < 0 && local_status == PathSelectState::AFTER_SAMPLE)
    {
        selected_path = shared_state->getPath();
        if (selected_path < 0 || static_cast<size_t>(selected_path) >= path_num)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid selected path: {}", selected_path);
        }

        for (int i = 0; static_cast<size_t>(i) < outputs_ptrs.size(); ++i)
        {
            if (i == selected_path)
            {
                continue;
            }
            // close other output ports
            outputs_ptrs[i]->finish();
        }
    }
    
    if (local_status == PathSelectState::SAMPLING) [[unlikely]]
    {
        if (input.isFinished())
        {
            input_finished = true;
            if (sampled_chunks.empty())
            {
                for (auto & port : outputs)
                {
                    if (!port.isFinished())
                    {
                        port.finish();
                    }
                }
                local_status = PathSelectState::AFTER_SAMPLE;
                selected_path = 0;
                shared_state->setPath(selected_path);
                shared_state->setStatus(PathSelectState::AFTER_SAMPLE);
                return Status::Finished;
            }
            return Status::Ready;
        }
        input.setNeeded();
        if (!input.hasData())
        {
            return Status::NeedData;
        }
        output_chunk = input.pull(true);
        has_input = true;
        return Status::Ready;
    }
    else if (local_status != PathSelectState::AFTER_SAMPLE)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status: {}", local_status);
    }

    return normalPrepare(input);
}

void MultiPathsSelectTransform::work()
{
    if (local_status == PathSelectState::AFTER_SAMPLE) [[likely]]
    {
        normalWork();
    }
    else
        samplingWork();
}

void MultiPathsSelectTransform::normalWork()
{
    if (has_output) [[unlikely]]
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected has_output = false");
    }
    if (!sampled_chunks.empty()) [[unlikely]]
    {
        output_chunk.swap(sampled_chunks.front());
        sampled_chunks.pop_front();
        has_input = !sampled_chunks.empty();
        has_output = true;
    }
    else if (has_input)
    {
        has_input = false;
        has_output = true;
    }
    else
    {
        LOG_INFO(logger, "No inputs");
    }
}

void MultiPathsSelectTransform::samplingWork()
{
    if (local_status == PathSelectState::WAIT_SAMPLE_FINISHED)
    {
        // is waiting for another processor to finish the sample.
        return;
    }
    if (has_input)
    {
        sampled_chunks.emplace_back(std::move(output_chunk));
        has_sample_rows += output_chunk.getNumRows();
    }
    if (has_sample_rows >= sample_rows_num || input_finished)
    {
        selected_path = path_selector->compute(sampled_chunks);
        local_status = PathSelectState::AFTER_SAMPLE;
        shared_state->setPath(selected_path);
        shared_state->setStatus(PathSelectState::AFTER_SAMPLE);
        LOG_INFO(logger, "{} {}Select path {} after sample {} blocks.", __LINE__, reinterpret_cast<UInt64>(this), selected_path, sampled_chunks.size());
        
        normalWork();
    }
    else
    {
        has_input = false;    
    }
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
