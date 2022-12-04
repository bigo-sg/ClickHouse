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
    IPathSampleSelector() = default;
    virtual ~IPathSampleSelector() = default;

    virtual Int32 getPath() = 0;
};

using IPathSampleSelectorPtr = std::shared_ptr<IPathSampleSelector>;

class MultiPathsSelectTransform : public IProcessor
{
public:
    explicit MultiPathsSelectTransform(
        const Block & header_,
        size_t path_num_,
        IPathSampleSelectorPtr path_selector_);
    ~MultiPathsSelectTransform() override = default;

    String getName() const override {return "MultiPathsSelectTransform"; }
    Status prepare() override;
    void work() override;
private:
    Block header;
    IPathSampleSelectorPtr path_selector;

    Int32 selected_path = -1;

    OutputPortRawPtrs outputs_ptrs;

    bool has_input = false;
    bool has_output = false;
    Chunk output_chunk;

    Poco::Logger * logger = &Poco::Logger::get("MultiPathsSelectTransform");
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
