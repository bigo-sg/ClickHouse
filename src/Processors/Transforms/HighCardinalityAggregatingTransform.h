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
    explicit HighCardinalityAggregatingTransform(const Block & header_, const Aggregator::Params & params_, bool final_);
    ~HighCardinalityAggregatingTransform() override = default;

    String getName() const override { return "HighCardinalityAggregatingTransform"; }
    Status prepare() override;
    void work() override;
private:
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

    Poco::Logger * logger = &Poco::Logger::get("HighCardinalityAggregatingTransform");

    bool isGenerateFinished() const;

    Block convertSingleLevel();
    Block convertTwoLevel(UInt32 bucket_num); 
};
}
