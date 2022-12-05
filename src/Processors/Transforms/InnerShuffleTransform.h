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
}
