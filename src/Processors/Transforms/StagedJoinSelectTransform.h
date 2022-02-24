#pragma once
#include <memory>
#include <mutex>
#include <Processors/IProcessor.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/ISource.h>
namespace DB
{

class StagedJoinInsertProcess
{
public:
    explicit StagedJoinInsertProcess(std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> insert_block_ios_);

    void runOnce();
private:
    std::mutex mutex;
    std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> insert_block_ios;
    std::atomic<bool> finished = false;
};

class StagedJoinInsertTransform : public ISource
{
public:
    explicit StagedJoinInsertTransform(std::shared_ptr<BlockIO> insert_block_io_);
    String getName() const override { return "StagedJoinInsertTransform"; }
    Chunk generate() override;
private:
    std::shared_ptr<BlockIO> insert_block_io;
    bool insert_finished = false;
};
class StagedJoinSelectTransform : public IProcessor
{
public:
    explicit StagedJoinSelectTransform(const Block & header_, std::shared_ptr<StagedJoinInsertProcess> insert_process_);
    String getName() const override { return "StagedJoinSelectTransform"; }
    Status prepare() override;
    void work() override;
private:
    std::shared_ptr<StagedJoinInsertProcess> insert_process;
    Chunk chunk;
    bool has_output = false;
    bool has_input = false;
};

class StagedJoinSelectTransformV2 : public IProcessor
{
public:
    explicit StagedJoinSelectTransformV2(const Block & header_);
    String getName() const override { return "StagedJoinSelectTransformV2"; }
    Status prepare() override;
    void work() override;
private:
    Chunk chunk;
    bool has_output = false;
    bool has_input = false;
};
}
