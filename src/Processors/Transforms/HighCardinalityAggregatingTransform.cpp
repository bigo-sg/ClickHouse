#include "HighCardinalityAggregatingTransform.h"
#include <memory>
#include <Interpreters/Aggregator.h>
#include <Processors/Formats/Impl/ORCBlockOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

static Block getOutputHeader(const Block & header, const Aggregator::Params & params, bool final)
{
    auto transform_params = std::make_shared<AggregatingTransformParams>(header, params, final);
    return transform_params->getHeader();
}

HighCardinalityAggregatingTransform::HighCardinalityAggregatingTransform(const Block & header_, const Aggregator::Params & params_, bool final_)
    : IProcessor({header_}, {getOutputHeader(header_, params_, final_)})
    , header(header_)
    , params(params_)
    , final(final_)
    , key_columns(params_.keys_size)
    , aggregate_columns(params_.aggregates_size)
{
    aggregate_data = std::make_shared<AggregatedDataVariants>();
    aggregator = std::make_unique<Aggregator>(header, params);
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
    
    bool is_input_finshed = input.isFinished();
    if (is_input_finshed || is_consume_finished)
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
    else if (!is_input_finshed)
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
            if (!num_rows && params.empty_result_for_aggregation_by_empty_set)
                return;
            if (!aggregator->executeOnBlock(input_chunk.detachColumns(), 0, num_rows, *aggregate_data, key_columns, aggregate_columns, no_more_keys))
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
    Block block = aggregator->prepareBlockAndFillSingleLevel<true>(*aggregate_data, final);
    return block;
}

Block HighCardinalityAggregatingTransform::convertTwoLevel(UInt32 bucket_num)
{

    ManyAggregatedDataVariants datas = {aggregate_data};
    Block block = aggregator->mergeAndConvertOneBucketToBlock(
        datas, aggregate_data->aggregates_pool, final, bucket_num, &is_cancelle);
    return block;
}
}
