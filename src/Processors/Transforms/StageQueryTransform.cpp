#include <memory>
#include <Processors/Transforms/StageQueryTransform.h>
#include <Common/logger_useful.h>
#include "Common/Stopwatch.h"
#include "Common/ThreadPool.h"
#include <Common/ErrorCodes.h>
#include "Processors/Executors/PullingAsyncPipelineExecutor.h"
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Core/Block.h>
#include <Parsers/queryToString.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <base/defines.h>
#include <Interpreters/Context.h>

#include <Processors/Executors/CompletedPipelineExecutor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
BlockIOSourceTransform::BlockIOSourceTransform(ContextPtr context_, const QueryBlockIO & block_io_)
    : ISource(block_io_.block_io->pipeline.completed() ? Block{} : block_io_.block_io->pipeline.getHeader())
    , context(context_)
    , block_io(block_io_.block_io)
    , query(block_io_.query)
{
    is_pulling_pipeline = block_io->pipeline.pulling();
    is_completed_pipeline = block_io->pipeline.completed();
}

BlockIOSourceTransform::~BlockIOSourceTransform()
{
    LOG_TRACE(logger, "run query({}) in elapsedMilliseconds:{}", queryToString(query), elapsed);
}

Chunk BlockIOSourceTransform::generate()
{
    if (unlikely(!watch))
    {
        watch = std::make_unique<Stopwatch>();
    }
    if (is_completed_pipeline)
    {
        LOG_TRACE(logger, "Run in completed mode. current query:{}", queryToString(query));
        CompletedPipelineExecutor executor(block_io->pipeline);
        executor.execute();
        watch->stop();
        elapsed = watch->elapsedMilliseconds();
        return {};
    }
    else if (is_pulling_pipeline)
    {
        if (!pulling_executor)
        {
            LOG_TRACE(logger, "Run in pulling mode. current query:{}", queryToString(query));
            pulling_executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io->pipeline);
        }
        Chunk res;
        while (pulling_executor->pull(res))
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
    watch->stop();
    elapsed = watch->elapsedMilliseconds();
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
StageBlockIOsConnectTransform::StageBlockIOsConnectTransform(
    ContextPtr context_, const QueryBlockIO & output_block_io_, const std::vector<Block> & input_headers_)
    : IProcessor(headersToInputPorts(input_headers_), {output_block_io_.block_io->pipeline.getHeader()})
    , context(context_)
    , output_block_io(output_block_io_.block_io)
    , query(output_block_io_.query)
{
    is_pulling_pipeline = output_block_io->pipeline.pulling();
    is_completed_pipeline = output_block_io->pipeline.completed();
}

StageBlockIOsConnectTransform::~StageBlockIOsConnectTransform()
{
    LOG_TRACE(logger, "run query({}) in elapsedMilliseconds:{}", queryToString(query), elapsed);
}

IProcessor::Status StageBlockIOsConnectTransform::prepare()
{
    if (unlikely(!watch))
    {
        watch = std::make_unique<Stopwatch>();
    }
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
        {
            input.close();
        }
        LOG_TRACE(logger, "output.isFinished()");
        watch->stop();
        elapsed = watch->elapsedMilliseconds();
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
        //LOG_TRACE(logger, "has_output");
        return Status::PortFull;
    }

    if (has_input)
    {
        //LOG_TRACE(logger, "has_input");
        return Status::Ready;
    }

    bool all_input_finished = true;
    for (auto & input : inputs)
    {
        if (!input.isFinished())
        {
            LOG_TRACE(logger, "try to pull upstream data. current query:{}", queryToString(query));
            all_input_finished = false;
            input.setNeeded();
            if (input.hasData())
                (void)input.pullData();
        }
    }

    if (!all_input_finished)
    {
        LOG_TRACE(logger, "need_blocked && !all_input_finished");
        return Status::NeedData;
    }

    if (is_completed_pipeline)
    {
        LOG_TRACE(logger, "Run in completed mode. current query:{}", queryToString(query));
        CompletedPipelineExecutor executor(output_block_io->pipeline);
        executor.execute();
        outputs.front().finish();
        watch->stop();
        elapsed = watch->elapsedMilliseconds();
        return Status::Finished;
    }
    else if (is_pulling_pipeline)
    {
        if (!pulling_executor)
        {
            LOG_TRACE(logger, "Run in pullig mode. current query:{}", queryToString(query));
            pulling_executor = std::make_unique<PullingAsyncPipelineExecutor>(output_block_io->pipeline);
        }
        Chunk new_chunk;
        while (pulling_executor->pull(new_chunk))
        {
            LOG_TRACE(logger, "pull chunk rows:{}", new_chunk.getNumRows());
            if (new_chunk)
            {
                has_input = true;
                chunk.swap(new_chunk);
                return Status::Ready;
            }
        }
        LOG_TRACE(logger, "pulling_executor->pull() = false. chunk rows:{}", new_chunk.getNumRows());
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid pipeline");
    }

    outputs.front().finish();
    watch->stop();
    elapsed = watch->elapsedMilliseconds();
    return Status::Finished;
}

void StageBlockIOsConnectTransform::work()
{
    if (has_input)
    {
        has_input = false;
        has_output = true;
    }
}

ParallelStageBlockIOsTransform::ParallelStageBlockIOsTransform(
    ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_)
    : ISource(output_block_io_.block_io->pipeline.completed() ? Block{} : output_block_io_.block_io->pipeline.getHeader())
    , context(context_)
    , output_block_io(output_block_io_)
    , input_block_ios(input_block_ios_)
{
    is_completed_pipeline = output_block_io.block_io->pipeline.completed();
    is_pulling_pipeline = output_block_io.block_io->pipeline.pulling();
}

ParallelStageBlockIOsTransform::~ParallelStageBlockIOsTransform()
{
    if (thread_pool)
        thread_pool->wait();
    LOG_TRACE(logger, "run query({}) in elapsedMilliseconds:{}", queryToString(output_block_io.query), elapsed);
}

Chunk ParallelStageBlockIOsTransform::generate()
{
    if (unlikely(!has_start_background_tasks))
    {
        startBackgroundTasks();
    }

    if (unlikely(!watch))
    {
        watch = std::make_unique<Stopwatch>();
    }

    if (is_completed_pipeline)
    {
        CompletedPipelineExecutor executor(output_block_io.block_io->pipeline);
        executor.execute();
        elapsed = watch->elapsedMilliseconds();
        return {};
    }
    else if (is_pulling_pipeline)
    {
        if (unlikely(!pulling_executor))
        {
            pulling_executor = std::make_unique<PullingAsyncPipelineExecutor>(output_block_io.block_io->pipeline);
        }
        Chunk res;
        while (pulling_executor->pull(res))
        {
            if (res)
            {
                LOG_TRACE(logger, "read chunk . rows:{}. query:{}", res.getNumRows(), queryToString(output_block_io.query));
                return res;
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid pipeline mode");
    }
    elapsed = watch->elapsedMilliseconds();
    return {};
}

void ParallelStageBlockIOsTransform::startBackgroundTasks()
{
    auto build_task = [](QueryBlockIO & block_io)
    {
        Stopwatch task_watch;
        if (block_io.block_io->pipeline.completed())
        {
            CompletedPipelineExecutor executor(block_io.block_io->pipeline);
            executor.execute();
        }
        else if (block_io.block_io->pipeline.pulling())
        {
            PullingAsyncPipelineExecutor executor(block_io.block_io->pipeline);
            Chunk res;
            while (executor.pull(res))
            {
            }
        }
        LOG_TRACE(
            &Poco::Logger::get("ParallelStageBlockIOsTransform"),
            "upstream query({}) run in elapsedMilliseconds:{}",
            queryToString(block_io.query),
            task_watch.elapsedMilliseconds());
    };
    thread_pool = std::make_unique<ThreadPool>(input_block_ios.size());
    for (auto & block : input_block_ios)
    {
        thread_pool->scheduleOrThrowOnError([&]() { build_task(block); });
    }
    has_start_background_tasks = true;
}
}
