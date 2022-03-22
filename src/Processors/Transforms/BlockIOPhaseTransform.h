#pragma once
#include <memory>
#include <mutex>
#include <Processors/IProcessor.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/ISource.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Poco/Logger.h>
namespace DB
{
class SourceBlockIOPhaseTransform : public ISource
{
public:
    explicit SourceBlockIOPhaseTransform(std::shared_ptr<BlockIO> block_io_);
    String getName() const override { return "SourceBlockIOPhaseTransform"; }
    Chunk generate() override;
private:
    std::shared_ptr<BlockIO> block_io;
    bool run_finished;
    bool is_pulling_pipeline = false;
    bool is_completed_pipeline = false;
    std::unique_ptr<PullingAsyncPipelineExecutor> pulling_executor;
    Poco::Logger * logger = &Poco::Logger::get("SourceBlockIOPhaseTransform");
};

class WaitBlockIOPhaseFinishedTransform : public IProcessor
{
public:
    explicit WaitBlockIOPhaseFinishedTransform(InputPorts input_ports);
    String getName() const override { return "WaitBlockIOPhaseFinishedTransform"; }
    Status prepare() override;
    void work() override;
private:
    std::list<InputPort*> running_inputs;
    Poco::Logger * logger = &Poco::Logger::get("WaitBlockIOPhaseFinishedTransform");
};

class SignalBlockIOPhaseFinishedTransform : public IProcessor
{
public:
    explicit SignalBlockIOPhaseFinishedTransform(const Block & header, size_t down_stream_size);
    String getName() const override { return "SignalBlockIOPhaseFinishedTransform"; }
    Status prepare() override;
    void work() override;

    static OutputPorts makeOutputPorts(const Block & header, size_t down_stream_size);
private:
    Poco::Logger * logger = &Poco::Logger::get("SignalBlockIOPhaseFinishedTransform");
};


class BlockIOPhaseTransform : public IProcessor
{
public:
    explicit BlockIOPhaseTransform(std::shared_ptr<BlockIO> block_io_, const Block & input_header_);
    String getName() const override { return "BlockIOPhaseTransform"; }
    Status prepare() override;
    void work() override;
private:
    Chunk chunk;
    bool has_output = false;
    bool has_input = false;
    std::shared_ptr<BlockIO> block_io;
    bool is_pulling_pipeline = false;
    bool is_completed_pipeline = false;
    std::unique_ptr<PullingAsyncPipelineExecutor> pulling_executor;
    Poco::Logger * logger = &Poco::Logger::get("BlockIOPhaseTransform");
};
}
