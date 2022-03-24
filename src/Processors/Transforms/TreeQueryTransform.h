#pragma once
#include <memory>
#include <Processors/ISource.h>
#include <QueryPipeline/BlockIO.h>
#include <Poco/Logger.h>
#include "Processors/IProcessor.h"
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
namespace DB
{
class BlockIOSourceTransform : public ISource
{
public:
    using BlockIOPtr = std::shared_ptr<BlockIO>;
    explicit BlockIOSourceTransform(BlockIOPtr block_io_);
    String getName() const override { return "BlockIOSourceTransform"; }
    Chunk generate() override;
private:
    BlockIOPtr block_io;
    bool is_pulling_pipeline;
    bool is_completed_pipeline;
    std::unique_ptr<PullingAsyncPipelineExecutor> pulling_executor;
    Poco::Logger * logger = &Poco::Logger::get("BlockIOSourceTransform");
};

class TreeBlockIOsConnectTransform : public IProcessor
{
public:
    using BlockIOPtr = std::shared_ptr<BlockIO>;    
    using BlockIOs = std::vector<BlockIOPtr>;
    TreeBlockIOsConnectTransform(BlockIOPtr output_block_io_, const std::vector<Block> & input_headers_, bool need_blocked_ = false);
    String getName() const override { return "TreeBlockIOsConnectTransform"; }
    Status prepare() override;
    void work() override;
private:
    BlockIOPtr output_block_io;
    bool need_blocked;
    bool is_pulling_pipeline;
    bool is_completed_pipeline;
    bool has_output = false;
    bool has_input = false;
    Chunk chunk;
    std::unique_ptr<PullingAsyncPipelineExecutor> pulling_executor;
    Poco::Logger * logger = &Poco::Logger::get("TreeBlockIOsConnectTransform");
    
};
}
