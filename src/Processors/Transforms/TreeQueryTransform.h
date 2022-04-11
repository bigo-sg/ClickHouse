#pragma once
#include <memory>
#include <Parsers/IAST_fwd.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <QueryPipeline/BlockIO.h>
#include <Poco/Logger.h>
#include <Common/Stopwatch.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <Core/BackgroundSchedulePool.h>
namespace DB
{

struct QueryBlockIO
{
    using BlockIOPtr = std::shared_ptr<BlockIO>;
    BlockIOPtr block_io;
    ASTPtr query; // the query to generate this block_io
};
using QueryBlockIOs = std::vector<QueryBlockIO>;
class BlockIOSourceTransform : public ISource
{
public:
    using BlockIOPtr = std::shared_ptr<BlockIO>;
    explicit BlockIOSourceTransform(ContextPtr context_, const QueryBlockIO & block_io_);
    ~BlockIOSourceTransform() override;
    String getName() const override { return "BlockIOSourceTransform"; }
    Chunk generate() override;
private:
    ContextPtr context;
    BlockIOPtr block_io;
    ASTPtr query;
    bool is_pulling_pipeline;
    bool is_completed_pipeline;
    std::unique_ptr<PullingAsyncPipelineExecutor> pulling_executor;
    Poco::Logger * logger = &Poco::Logger::get("BlockIOSourceTransform");
    std::unique_ptr<Stopwatch> watch;
    UInt64 elapsed = 0l;
};

class TreeBlockIOsConnectTransform : public IProcessor
{
public:
    using BlockIOPtr = std::shared_ptr<BlockIO>;    
    using BlockIOs = std::vector<BlockIOPtr>;
    TreeBlockIOsConnectTransform(
        ContextPtr context_,
        const QueryBlockIO & output_block_io_,
        const std::vector<Block> & input_headers_,
        bool need_blocked_ = false);
    ~TreeBlockIOsConnectTransform() override;
    String getName() const override { return "TreeBlockIOsConnectTransform"; }
    Status prepare() override;
    void work() override;
private:
    ContextPtr context;
    BlockIOPtr output_block_io;
    ASTPtr query;
    bool need_blocked;
    bool is_pulling_pipeline;
    bool is_completed_pipeline;
    bool has_output = false;
    bool has_input = false;
    bool has_trigger_inputs = false;
    Chunk chunk;
    std::unique_ptr<PullingAsyncPipelineExecutor> pulling_executor;
    Poco::Logger * logger = &Poco::Logger::get("TreeBlockIOsConnectTransform");
    std::unique_ptr<Stopwatch> watch;
    UInt64 elapsed = 0l; 
};

class ParallelTreeBlockIOsTransform : public ISource
{
public:
    explicit ParallelTreeBlockIOsTransform(ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_);
    ~ParallelTreeBlockIOsTransform() override;
    String getName() const override { return "ParallelTreeBlockIOsTransform"; }
    Chunk generate() override;
private:
    ContextPtr context;
    QueryBlockIO output_block_io;
    QueryBlockIOs input_block_ios;

    bool is_pulling_pipeline;
    bool is_completed_pipeline;
    
    Chunk chunk;
    std::unique_ptr<PullingAsyncPipelineExecutor> pulling_executor;
    Poco::Logger * logger = &Poco::Logger::get("ParallelTreeBlockIOsTransform");

    bool has_start_background_tasks = false;
    std::vector<BackgroundSchedulePool::TaskHolder> background_tasks;
    void startBackgroundTasks();

    std::unique_ptr<Stopwatch> watch;
    UInt64 elapsed = 0l;
};
}
