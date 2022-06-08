#include <memory>
#include <Processors/Transforms/HighCardinalityAggregatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include "Interpreters/Aggregator.h"
#include "Processors/Formats/Impl/ORCBlockOutputFormat.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

HighCardinalityAggregatingTransform::HighCardinalityAggregatingTransform(const Block & header, std::unique_ptr<Params> params_)
    : IProcessor({header}, {params_->getHeader()})
    , params(std::move(params_))
    , key_columns(params->aggregator_params.keys_size)
    , aggregate_columns(params->aggregator_params.aggregates_size)
{
    aggregate_data = std::make_shared<AggregatedDataVariants>();
}

IProcessor::Status HighCardinalityAggregatingTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }
    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
        return Status::PortFull;
    
    bool is_input_finoshed = input.isFinished();
    if (is_input_finoshed || is_consume_finished)
    {
        if (!is_consume_finished) [[unlikely]]
        {
            is_consume_finished = true;
        }

        if (isGenerateFinished())
        {
            output.finish();
            return Status::Finished;
        }

        return Status::Ready;
    }
    else if (!is_input_finoshed)
    {
        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }
        input_chunk = input.pull();
        has_input = true;

        return Status::Ready;
    }

    return Status::Ready;
}

void HighCardinalityAggregatingTransform::work()
{
    if (!is_consume_finished)
    {
        if (has_input)
        {
            has_input = false;
            size_t num_rows = input_chunk.getNumRows();
            src_rows += num_rows;
            src_bytes += input_chunk.bytes();
            if (!num_rows && params->aggregator_params.empty_result_for_aggregation_by_empty_set)
                return;
            if (!params->aggregator->executeOnBlock(input_chunk.detachColumns(), 0, num_rows, *aggregate_data, key_columns, aggregate_columns, no_more_keys))
            {
                is_consume_finished = true;
            }
        }
    }
    else
    {
        // It's a two-level hashmap
        if (aggregate_data->isTwoLevel())
        {
            current_bucket_num += 1;
            if (current_bucket_num >= NUM_BUCKETS)
            {
                is_generate_finished = true;
                return;
            }
            output_chunk = convertToChunk(convertTwoLevel(current_bucket_num));
            if (!output_chunk.getNumRows())
                return;
            has_output = true;
        }
        else
        {
            output_chunk = convertToChunk(convertSingleLevel());
            is_generate_finished = true;
            if (!output_chunk.getNumRows())
                return;
            has_output = true;
        }
    }
}

bool HighCardinalityAggregatingTransform::isGenerateFinished() const
{
    return is_generate_finished;
}

Block HighCardinalityAggregatingTransform::convertSingleLevel()
{
    if (aggregate_data->type == AggregatedDataVariants::Type::EMPTY)
        return getInputs().front().getHeader().cloneEmpty();
    Block block = params->aggregator->prepareBlockAndFillSingleLevel(*aggregate_data, params->final);
    return block;
}

Block HighCardinalityAggregatingTransform::convertTwoLevel(UInt32 bucket_num)
{

    ManyAggregatedDataVariants datas = {aggregate_data};
    Block block = params->aggregator->mergeAndConvertOneBucketToBlock(
        datas, aggregate_data->aggregates_pool, params->final, bucket_num, &is_cancelle);
    return block;
}
}
