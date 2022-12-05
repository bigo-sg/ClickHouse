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

    void setNumStreams(size_t n ) { num_streams = n; }
protected:
    size_t num_streams = 0;
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
}
