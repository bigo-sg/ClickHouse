#include "SampleStatisticsTransform.h"
#include <mutex>
#include <Analyzer/BlockStatAnalyzer.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SampleStatisticsTransform::SampleStatisticsTransform(
    const Block & header_, std::shared_ptr<SharedState> shared_state_, size_t sample_rows_num_)
    : IProcessor({header_}, {header_})
    , header(header_)
    , shared_state(shared_state_)
    , sample_rows_num(sample_rows_num_)
{}

IProcessor::Status SampleStatisticsTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (local_status != ProcessStatus::AFTER_SAMPLING) [[unlikely]]
    {
        return samplingPrepare(input);
    }
    return normalPrepare(input);
}

IProcessor::Status SampleStatisticsTransform::samplingPrepare(InputPort & input)
{
    if (local_status < ProcessStatus::DO_SAMPLING)
    {
        std::lock_guard lock(shared_state->mutex);
        ProcessStatus current_status = shared_state->process_status;
        if (current_status == ProcessStatus::BEFORE_SAMPLING)
        {
            // only one processor do the sample. other processors will wait for this
            // processor finishing sampling.
            local_status = ProcessStatus::DO_SAMPLING;
            shared_state->process_status = local_status;
        }
        else if (current_status == ProcessStatus::DO_SAMPLING)
        {
            // other processors that do not sample, just wait the previous one finished.
            local_status = ProcessStatus::WAIT_SAMPLED;
        }
        else
        {
            local_status = current_status;
        }
    }

    if (local_status == ProcessStatus::WAIT_SAMPLED)
    {
        ProcessStatus current_status = shared_state->process_status;
        if (current_status == ProcessStatus::DO_SAMPLING)
        {
            // let it run idlly
            return Status::Ready;
        }
        local_status = current_status;
    }

    if (local_status == ProcessStatus::DO_SAMPLING)
    {
        if (input.isFinished())
        {
            input_finished = true;
            if (sampled_chunks.empty())
            {
                LOG_ERROR(logger, "Cannot finish sample statistics");
                outputs.front().finish();
                shared_state->process_status = ProcessStatus::AFTER_SAMPLING;
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

    // else do normal prepare
    assert(local_status == ProcessStatus::AFTER_SAMPLING);
    return normalPrepare(input);
}

IProcessor::Status SampleStatisticsTransform::normalPrepare(InputPort & input)
{
    if (has_input)
        return Status::Ready;
    if (input.isFinished())
    {
        outputs.front().finish();
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

void SampleStatisticsTransform::work()
{
    if (local_status != ProcessStatus::AFTER_SAMPLING) [[unlikely]]
    {
        samplingWork();
    }
    else
        normalWork();
}

void SampleStatisticsTransform::samplingWork()
{
    if (local_status == ProcessStatus::WAIT_SAMPLED)
        return;
    if (has_input)
    {
        has_sample_rows += output_chunk.getNumRows();
        sampled_chunks.emplace_back(std::move(output_chunk));
    }

    if (has_sample_rows >= sample_rows_num || input_finished)
    {
        std::vector<Block> blocks;
        for (const auto & chunk : sampled_chunks)
        {
            auto cols = chunk.getColumns();
            ColumnsWithTypeAndName cols_with_name_type;
            for (size_t i = 0; i < cols.size(); ++i)
            {
                auto & block_col = header.getByPosition(i);
                ColumnWithTypeAndName col_with_name_type(cols[i], block_col.type, block_col.name);
                cols_with_name_type.emplace_back(col_with_name_type);
            }
            blocks.emplace_back(cols_with_name_type);
        }
        BlockStatAnalyzer analyzer(blocks);
        if (!shared_state->stat) [[unlikely]]
            shared_state->stat = std::make_shared<BlockStatMetadata>();
        *shared_state->stat = analyzer.analyze();

        local_status = ProcessStatus::AFTER_SAMPLING;
        shared_state->process_status = local_status;
        LOG_INFO(logger, "{}. sample rows:{}, stat result:{}", __LINE__, has_sample_rows, shared_state->stat->debugString());
        normalWork();
    }
    else
    {
        has_input = false;
    }
}

void SampleStatisticsTransform::normalWork()
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
}
