#pragma once
#include <memory>
#include <Processors/Transforms/MultiPathsSelectTransform.h>
#include <Interpreters/Aggregator.h>
#include <Analyzer/BlockStatAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MultiPathSelectStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/Pipe.h>
namespace DB
{
enum CardinalityType
{
    UNKNOWN = -1,
    LOW = 0,
    HIGH,
};

class AggregatingAlgorithmSelector : public IPathSampleSelector
{
public:
    explicit AggregatingAlgorithmSelector(
        std::shared_ptr<BlockStatMetadata> stat_info_,
        Aggregator::Params params_,
        UInt32 high_card_threshold_ = 90);
    ~AggregatingAlgorithmSelector() override = default;

    Int32 getPath() override;

private:
    std::shared_ptr<BlockStatMetadata> stat_info;
    Aggregator::Params params;
    UInt32 high_card_threshold;
};

class LowCardinalityAggregatingExecution : public DynamicPathBuilder
{
public:
    explicit LowCardinalityAggregatingExecution(
        Aggregator::Params params_,
        size_t merge_threads_,
        bool final_,
        size_t temporary_data_merge_threads_);
    ~LowCardinalityAggregatingExecution() override = default;

    DynamicPathBuilder::PathResult buildPath(size_t num_streams, const OutputPortRawPtrs & outports) override;
private:
    Aggregator::Params params;
    size_t merge_threads;
    bool final;
    size_t temporary_data_merge_threads;
};

class HighCardinalityAggregatingExecution : public DynamicPathBuilder
{
public:
    explicit HighCardinalityAggregatingExecution(
        Aggregator::Params params_,
        bool final_);
    ~HighCardinalityAggregatingExecution() override = default;

    DynamicPathBuilder::PathResult buildPath(size_t num_streams, const OutputPortRawPtrs & outports) override;
private:
    Aggregator::Params params;
    bool final;
};

}
