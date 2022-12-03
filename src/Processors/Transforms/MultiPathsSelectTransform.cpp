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
std::unique_ptr<std::lock_guard<std::mutex>> PathSelectState::requiredSamplingLock()
{
    auto lock = std::make_unique<std::lock_guard<std::mutex>>(sampling_mutex);
    return lock;
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
        size_t sample_blocks_num_)
    : IProcessor({header_}, splitOutputport(path_num_, header_))
    , header(header_)
    , path_num(path_num_)
    , shared_state(shared_state_)
    , path_selector(path_selector_)
    , sample_blocks_num(sample_blocks_num_)
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
        if (local_status == PathSelectState::AFTER_SAMPLE)
        {
            sample_lock.reset();
        }
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
        LOG_ERROR(logger, "xxxx {} chunk: {} {};{} {}", __LINE__, output_chunk.getNumRows(), output_chunk.getNumColumns(), reinterpret_cast<UInt64>(this), sampled_chunks.size());
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
    if (local_status == PathSelectState::BEFORE_SAMPLE)
    {
        local_status = shared_state->getStatus();
    }
    if (local_status < PathSelectState::SAMPLING)
    {
        LOG_ERROR(logger, "xxxx state:{}", reinterpret_cast<UInt64>(shared_state.get()));
        sample_lock = shared_state->requiredSamplingLock();
        // shared_state->getSamplingMutex().lock();
        local_status = shared_state->getStatus();
        if (local_status == PathSelectState::BEFORE_SAMPLE)
        {
            // only one thread do the sample. other threads will wait for this
            // thread finishing sampling.
            LOG_ERROR(logger, "xxxx acquired sampling. {}", reinterpret_cast<UInt64>(this));
            local_status = PathSelectState::SAMPLING;
        }
        else if (local_status == PathSelectState::AFTER_SAMPLE)
        {
            sample_lock.reset();
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status here:{}", local_status);    
        }
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
        if (has_input)
            return Status::Ready;
        if (input.isFinished())
        {
            input_finished = true;
            if (sampled_chunks.empty())
            {
                LOG_ERROR(logger, "xxxx {} input finished. {}", __LINE__, reinterpret_cast<UInt64>(this), sampled_chunks.size());
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
                sample_lock.reset();
                return Status::Finished;
            }
            LOG_ERROR(logger, "xxxx {} input finished. {}", __LINE__, reinterpret_cast<UInt64>(this), sampled_chunks.size());
            return Status::Ready;
        }
        input.setNeeded();
        if (!input.hasData())
        {
            LOG_ERROR(logger, "xxxx {} requrie one more block. {} {}", __LINE__, reinterpret_cast<UInt64>(this), sampled_chunks.size());
            return Status::NeedData;
        }
        output_chunk = input.pull(true);
        LOG_ERROR(logger, "xxxx {} sample one block. {} {}, chunks: {} {}", __LINE__, reinterpret_cast<UInt64>(this), sampled_chunks.size(), output_chunk.getNumRows(), output_chunk.getNumColumns());
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
    LOG_ERROR(logger, "xxxx {} {}, status:{}, has_input:{}", __LINE__, reinterpret_cast<UInt64>(this), local_status, has_input);
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
    LOG_ERROR(logger, "xxxx {} current sample chunks: {}. {}", __LINE__, sampled_chunks.size(), reinterpret_cast<UInt64>(this));
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
    LOG_ERROR(logger, "xxxx {}, {}, chunk:{}, {}", __LINE__, reinterpret_cast<UInt64>(this), output_chunk.getNumRows(), output_chunk.getNumColumns());
    if (has_input)
        sampled_chunks.emplace_back(std::move(output_chunk));
    if (sampled_chunks.size() >= sample_blocks_num || input_finished)
    {
        selected_path = path_selector->compute(sampled_chunks);
        local_status = PathSelectState::AFTER_SAMPLE;
        shared_state->setPath(selected_path);
        shared_state->setStatus(PathSelectState::AFTER_SAMPLE);
        sample_lock.reset();
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
