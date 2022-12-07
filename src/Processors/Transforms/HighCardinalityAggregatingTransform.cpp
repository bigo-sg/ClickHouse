#include "HighCardinalityAggregatingTransform.h"
#include <memory>
#include <Interpreters/Aggregator.h>
#include <Processors/Formats/Impl/ORCBlockOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include "Common/logger_useful.h"
#include "Core/Defines.h"
#include "Processors/Chunk.h"

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

HighCardinalityAggregatingTransform::~HighCardinalityAggregatingTransform()
{
    LOG_ERROR(logger, "xxxx consume rows:{}", src_rows);
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
            if (params.only_merge)
            {
                auto block = getInputs().front().getHeader().cloneWithColumns(input_chunk.detachColumns());
                block = materializeBlock(block);
                if (aggregator->mergeOnBlock(block, *aggregate_data, no_more_keys))
                    is_consume_finished = true;
            }
            else
            {
                if (!aggregator->executeOnBlock(
                        input_chunk.detachColumns(), 0, num_rows, *aggregate_data, key_columns, aggregate_columns, no_more_keys))
                {
                    is_consume_finished = true;
                }
            }
        }
    }
    else
    {
        // It's a two-level hashmap
        if (aggregate_data->isTwoLevel())
        {
            if (current_bucket_num >= NUM_BUCKETS)
            {
                LOG_ERROR(logger, "xxxx is two level hash map");
                is_generate_finished = true;
            }

            if (!is_generate_finished)
            {
                pending_blocks.emplace_back(convertTwoLevel(current_bucket_num));
                pending_rows += pending_blocks.back().rows();
                current_bucket_num += 1;
            }

            if (pending_rows >= DEFAULT_BLOCK_SIZE || is_generate_finished)
            {
                output_chunk = convertToChunk(concatenateBlocks(pending_blocks));
                pending_rows = 0;
                pending_blocks.clear();
            }

            if (!output_chunk.getNumRows())
                return;
            has_output = true;
        }
        else
        {
            LOG_ERROR(logger, "xxxx is single level hash map");
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

static InputPorts splitInputPort(size_t num_streams, const Block & header)
{
    InputPorts ports;
    for (size_t i = 0; i < num_streams; ++i)
    {
        InputPort port(header);
        ports.push_back(port);
    }
    return ports;
}

UnionStreamsTransform::UnionStreamsTransform(const Block & header_, size_t inputs_num)
    : IProcessor(splitInputPort(inputs_num, header_), {header_})
{
    for (auto & port : inputs)
    {
        running_inputs.emplace_back(&port);
    }
}

IProcessor::Status UnionStreamsTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto * input : running_inputs)
        {
            if (!input->isFinished())
                input->close();
        }
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto * input : running_inputs)
        {
            if (!input->isFinished())
            {
                input->setNotNeeded();
            }
        }
        return Status::PortFull;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    bool all_inputs_closed = true;
    for (auto it = running_inputs.begin(); it != running_inputs.end();)
    {
        auto * input = *it;
        if (input->isFinished())
        {
            running_inputs.erase(it++);
            continue;
        }
        it++;
        all_inputs_closed = false;
        input->setNeeded();
        if (!input->hasData())
        {
            continue;
        }
        input_chunks.emplace_back(input->pull(true));
        pending_rows += input_chunks.back().getNumRows();
        if (pending_rows >= DEFAULT_BLOCK_SIZE)
        {
            break;
        }
    }
    if (pending_rows >= DEFAULT_BLOCK_SIZE)
    {
        has_input = true;
        output_chunk = generateOneChunk();
    }
    else if (!all_inputs_closed)
    {
        for (auto * port : running_inputs)
        {
            if (!port->isFinished())
                port->setNeeded();
        }
        return Status::NeedData;
    }   

    if (all_inputs_closed) [[unlikely]]
    {
        if (pending_rows)
        {
            has_input = true;
            output_chunk = generateOneChunk();
            return Status::Ready;
        }
        for (auto & port : outputs)
        {
            if (!port.isFinished())
            {
                port.finish();
            }
        }
        return Status::Finished;
    }

    if (!has_input)
        return Status::NeedData;
    return Status::Ready;
}

Chunk UnionStreamsTransform::generateOneChunk()
{ 
    auto mutable_cols = input_chunks[0].mutateColumns();
    for (size_t col_index = 0, n = mutable_cols.size(); col_index < n; ++col_index)
    {
        mutable_cols[col_index]->reserve(pending_rows);
        for (size_t chunk_it = 1, m = input_chunks.size(); chunk_it < m; ++chunk_it)
        {
            const auto & src_cols = input_chunks[chunk_it].getColumns();
            mutable_cols[col_index]->insertRangeFrom(*src_cols[col_index], 0, input_chunks[chunk_it].getNumRows());
        }
    }
    auto chunk = Chunk(std::move(mutable_cols), pending_rows);
    input_chunks.clear();
    pending_rows = 0;
    return chunk;
}

void UnionStreamsTransform::work()
{
    if (has_input)
    {
        has_input = false;
        has_output = true;
    }
}
}
