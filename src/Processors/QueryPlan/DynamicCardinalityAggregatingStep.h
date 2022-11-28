#pragma once

#include <condition_variable>
#include <memory>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <llvm/Demangle/ItaniumDemangle.h>
#include <Processors/Port.h>
#include "Interpreters/Context.h"
#include "QueryPipeline/Pipe.h"
namespace DB
{
/**
 * When runs group by on high cardinality keys, the default implementation, AggregatingStep, has
 * bad performance. Since the implementation has a phase of merging maps from different threads.
 * The maps could be large when we have high cardinality keys, and the merging cost is large.
 * 
 * So we have a simple idea here for the high cardinarly group by.
 *   1) make a shuflle on the group by keys into different streams
 *   2) every stream runs a independent group by
 * There is no merging action here. But this implementation has bad performance on low cardinality
 * keys since the shuffle action.
 * 
 * The pipeline supports dynamically changed (expand pipeline). We first consume some blocks from
 * the up stream node, check that wether all the keys are low cardinality or not, then make the
 * final pipeline.
 */

class DynamicCardinalityAggregatingStep : public ITransformingStep
{
public:
    class SharedState
    {
    public:
        explicit SharedState(size_t num_streams_);
        enum Status
        {
            BEFORE_INIT_PIPELINE_TYPE,
            INITIALIZING_PIPELINE_TYPE,
            AFTER_INIT_PIPELINE,
            INITIALIZING_SHUFFLE,
            AFTER_INIT_SHUFFLE,
        };

        enum PipelineType
        {
            LOW_CARDINALITY,
            HIGH_CARDINALITY,
        };

        Status getStatus();
        void setStatus(Status status_);
        PipelineType getPipelineType();
        void setPipelineType(PipelineType type_);

        Status beginInitializePipelineType();
        void endInitializePipelineType();
        void beginBuildShuffle();
        void endBuildShuffle();

        OutputPortRawPtrs getShuffleOutports();
    private:
        size_t num_streams = 0;

        std::mutex shuffle_build_mutex;
        std::condition_variable shuffle_build_cond;
        std::atomic<int> shuffle_build_finished = 0;
        OutputPortRawPtrs shuffle_output_ports; // size = num_streams * num_streams

        std::mutex status_mutex;
        Status current_status = BEFORE_INIT_PIPELINE_TYPE;
        PipelineType pipeline_type = LOW_CARDINALITY;
    };
    using SharedStatePtr = std::shared_ptr<SharedState>;

    DynamicCardinalityAggregatingStep(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        bool final_,
        size_t max_block_size_,
        size_t aggregation_in_order_max_block_bytes_,
        size_t max_blocks_to_sample_ = 2,
        size_t high_card_threshold = 80);
    String getName() const override { return "DynamicCardinalityAggregatingStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;
private:
    Aggregator::Params params;
    bool final;
    //size_t max_block_size;
    //size_t aggregation_in_order_max_block_bytes;
    Processors aggregating;
};



}
