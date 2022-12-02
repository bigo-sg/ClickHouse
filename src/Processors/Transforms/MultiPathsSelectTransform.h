#pragma once

#include <memory>
#include <mutex>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <QueryPipeline/Pipe.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
namespace DB
{
class IPathSampleSelector
{
public:
    explicit IPathSampleSelector(const Block & header_) : header(header_) {}
    virtual ~IPathSampleSelector() = default;

    virtual Int32 compute(const std::list<Chunk> & samples) = 0;

private:
    Block header;
};
using IPathSampleSelectorPtr = std::shared_ptr<IPathSampleSelector>;

class PathSelectState
{
public:
    enum Status
    {
        BEFORE_SAMPLE,
        SAMPLING,
        AFTER_SAMPLE,
    };

    Status getStatus();
    void setStatus(Status status_);

    std::unique_ptr<std::lock_guard<std::mutex>> requiredSamplingLock();
    void leaveSampling();

    Int32 getPath();
    void setPath(Int32 path);
private:
    std::mutex sampling_mutex;

    std::atomic<Status> status = BEFORE_SAMPLE;

    std::atomic<Int32> selected_path = -1;
};

using PathSelectStatePtr = std::shared_ptr<PathSelectState>;

class MultiPathsSelectTransform : public IProcessor
{
public:
    explicit MultiPathsSelectTransform(
        const Block & header_,
        size_t path_num_,
        PathSelectStatePtr shared_state_,
        IPathSampleSelectorPtr path_selector_,
        size_t sample_blocks_num_ = 1);
    ~MultiPathsSelectTransform() override = default;

    String getName() const override {return "MultiPathsSelectTransform"; }
    Status prepare() override;
    void work() override;
private:
    Block header;
    size_t path_num;
    PathSelectStatePtr shared_state;
    IPathSampleSelectorPtr path_selector;

    Int32 selected_path = -1;
    PathSelectState::Status local_status = PathSelectState::BEFORE_SAMPLE;

    std::unique_ptr<std::lock_guard<std::mutex>> sample_lock;
    size_t sample_blocks_num;
    OutputPortRawPtrs outputs_ptrs;
    std::list<Chunk> sampled_chunks;

    bool input_finished = false;
    bool has_input = false;
    bool has_output = false;
    Chunk output_chunk;

    Poco::Logger * logger = &Poco::Logger::get("MultiPathsSelectTransform");

    Status normalPrepare(InputPort & input);
    Status samplingPrepare(InputPort & input);

    void normalWork();
    void samplingWork();
};

class UnionStreamsTransform : public IProcessor
{
public:
    explicit UnionStreamsTransform(const Block & header_, size_t inputs_num);
    ~UnionStreamsTransform() override = default;
    String getName() const override {return "UnionStreamsTransform"; }
    Status prepare() override;
    void work() override;
private:
    bool has_input = false;
    bool has_output = false;
    Chunk output_chunk;
    std::list<InputPort *> running_inputs;
};
}
