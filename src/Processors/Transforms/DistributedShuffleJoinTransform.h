#pragma once
#include <memory>
#include <mutex>
#include <Processors/IProcessor.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/ISource.h>
namespace DB
{

class DistributedShuffleJoinShufflePhase
{
public:
    explicit DistributedShuffleJoinShufflePhase(std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> insert_block_ios_);

    void runOnce();
private:
    std::mutex mutex;
    std::shared_ptr<std::vector<std::shared_ptr<BlockIO>>> insert_block_ios;
    std::atomic<bool> finished = false;
};

class DistributedShuffleJoinShuffleTransform : public ISource
{
public:
    explicit DistributedShuffleJoinShuffleTransform(std::shared_ptr<BlockIO> insert_block_io_);
    String getName() const override { return "DistributedShuffleJoinShuffleTransform"; }
    Chunk generate() override;
private:
    std::shared_ptr<BlockIO> insert_block_io;
    bool insert_finished = false;
};

class DistributedShuffleJoinTransform : public IProcessor
{
public:
    explicit DistributedShuffleJoinTransform(const Block & header_);
    String getName() const override { return "DistributedShuffleJoinTransform"; }
    Status prepare() override;
    void work() override;
private:
    Chunk chunk;
    bool has_output = false;
    bool has_input = false;
};
}
