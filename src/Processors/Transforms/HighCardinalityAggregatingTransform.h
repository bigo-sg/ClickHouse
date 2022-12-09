#pragma once
#include <memory>
#include <Interpreters/Aggregator.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>

namespace DB
{
class HighCardinalityAggregatingTransform : public IProcessor
{
public:
    explicit HighCardinalityAggregatingTransform(
        size_t id_, size_t num_streams_, const Block & header_, const Aggregator::Params & params_, bool final_);
    ~HighCardinalityAggregatingTransform() override;

    String getName() const override { return "HighCardinalityAggregatingTransform"; }
    Status prepare() override;
    void work() override;
private:
    size_t id;
    size_t num_streams;
    Block header;
    Aggregator::Params params;
    bool final;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    bool no_more_keys = false;
    std::atomic<bool> is_cancelle;
    std::unique_ptr<Aggregator> aggregator;

    Stopwatch watch;
    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;
    bool is_consume_finished = false;
    Chunk input_chunk;
    bool has_input = false;
    Chunk output_chunk;
    bool has_output = false;

    AggregatedDataVariantsPtr aggregate_data;

    UInt32 current_bucket_num = 0;
    static constexpr UInt32 NUM_BUCKETS = 256;
    bool is_generate_finished = false;
    std::vector<Block> pending_blocks;
    size_t pending_rows = 0;

    Poco::Logger * logger = &Poco::Logger::get("HighCardinalityAggregatingTransform");

    bool isGenerateFinished() const;

    Block convertSingleLevel();
    Block convertTwoLevel(UInt32 bucket_num);

    Chunk buildFiltedChunk(Chunk & input_chunk_);
};

class UnionStreamsTransform : public IProcessor
{
public:
    explicit UnionStreamsTransform(const Block & header_, size_t inputs_num);
    ~UnionStreamsTransform() override = default;
    String getName() const override { return "UnionStreamsTransform"; }
    Status prepare() override;
    void work() override;
private:
    bool has_input = false;
    bool has_output = false;
    Chunk output_chunk;
    std::vector<Chunk> input_chunks;
    size_t pending_rows = 0;
    std::list<InputPort *> running_inputs;

    Chunk generateOneChunk();

};

// This transform will append a new hash column into the original block.
// It shoul be remove after be used.
class BuildAggregatingKeysHashColumnTransform : public IProcessor
{
public:
    explicit BuildAggregatingKeysHashColumnTransform(const Block & header_, const std::vector<size_t> & hash_columns_, size_t num_streams_);
    ~BuildAggregatingKeysHashColumnTransform() override = default;
    String getName() const override { return "BuildAggregatingKeysHashColumnTransform"; }
    Status prepare() override;
    void work() override;
private:
    Block header;
    std::vector<size_t> hash_columns;
    size_t num_streams;
    Block output_header;
    bool has_input = false;
    bool has_output = false;
    Chunk output_chunk;
    String hash_column_name;

};
}
