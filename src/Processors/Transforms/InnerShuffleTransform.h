#pragma once
#include <Processors/IProcessor.h>
#include <Poco/Logger.h>
namespace DB
{
class InnerShuffleTransform : public IProcessor
{
public:
    InnerShuffleTransform(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_);
    String getName() const override { return "InnerShuffleTransform"; }
    Status prepare() override;
    void work() override;

private:
    size_t num_streams;
    Block header;
    std::vector<size_t> hash_columns;
    bool has_output = false;
    std::list<Chunk> output_chunks;
    bool has_input = false;
    Chunk input_chunk;

    //Poco::Logger * logger = &Poco::Logger::get("InnerShuffleTransform");
};

class InnerShuffleCollectTransform : public IProcessor
{
public:
    InnerShuffleCollectTransform(size_t num_streams_, const Block & header_);
    String getName() const override { return "InnerShuffleCollectTransform"; }
    Status prepare() override;
    void work() override;
private:
    Block header;
    bool has_output = false;
    std::list<Chunk> input_chunks;
    Chunk output_chunk;
    bool has_input = false;

    //Poco::Logger * logger = &Poco::Logger::get("InnerShuffleCollectTransform");
};
}
