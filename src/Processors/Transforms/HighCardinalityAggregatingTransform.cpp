#include "HighCardinalityAggregatingTransform.h"
#include <memory>
#include <Interpreters/Aggregator.h>
#include <Processors/Formats/Impl/ORCBlockOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include "Common/typeid_cast.h"
#include <Common/WeakHash.h>
#include <Common/logger_useful.h>
#include "Columns/IColumn.h"
#include "base/types.h"
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

static Block getOutputHeader(const Block & original_header, const Aggregator::Params & params, bool final)
{
    // remove the aggregating keys hash column from BuildAggregatingKeysHashColumnTransform
    const ColumnsWithTypeAndName & old_named_cols = original_header.getColumnsWithTypeAndName();
    ColumnsWithTypeAndName new_named_cols;
    new_named_cols.insert(new_named_cols.end(), old_named_cols.begin(), old_named_cols.begin() + old_named_cols.size() - 1);
    Block header(new_named_cols);
    auto transform_params = std::make_shared<AggregatingTransformParams>(header, params, final);
    return transform_params->getHeader();
}

HighCardinalityAggregatingTransform::HighCardinalityAggregatingTransform(
    size_t id_, size_t num_streams_, const Block & header_, const Aggregator::Params & params_, bool final_)
    : IProcessor({header_}, {getOutputHeader(header_, params_, final_)})
    , id(id_)
    , num_streams(num_streams_)
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
                #if 1
                auto input_cols = input_chunk.detachColumns();
                Columns used_cols(input_cols.begin(), input_cols.begin() + input_cols.size() - 1);
                auto hash_col = input_cols[input_cols.size() - 1];
                #if 0
                auto flag_col = ColumnUInt8::create(num_rows,0);
                auto & flag_array = flag_col->getData();

                const ColumnUInt32 * uint32_col = typeid_cast<const ColumnUInt32 *>(hash_col->getPtr().get());
                const auto & array = uint32_col->getData();
                for (size_t i = 0; i < num_rows; ++i)
                {
                    flag_array[i] = array[i] == id;
                }
                #endif
                //if (!aggregator->executeOnBlock(
                //        used_cols, 0, num_rows, *aggregate_data, key_columns, aggregate_columns, no_more_keys, hash_col->getPtr().get(), id))
                //{
                //    is_consume_finished = true;
                //}
                #else
                // auto filted_chunk = buildFiltedChunk(input_chunk);
                // auto cols = filted_chunk.detachColumns();
                auto cols = input_chunk.detachColumns();
                if (!aggregator->executeOnBlock(
                        cols, 0, cols[0]->size(), *aggregate_data, key_columns, aggregate_columns, no_more_keys))
                {
                    is_consume_finished = true;
                }
                #endif
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

Chunk HighCardinalityAggregatingTransform::buildFiltedChunk(Chunk & input_chunk_)
{
    size_t num_rows = input_chunk_.getNumRows();
    auto input_cols = input_chunk.detachColumns();
    Columns used_cols(input_cols.begin(), input_cols.begin() + input_cols.size() - 1);
    std::vector<MutableColumnPtr> filted_cols(used_cols.size());
    size_t reserve_size = static_cast<size_t>(num_rows * 1.5 / num_streams);
    const ColumnUInt8 * hash_col = typeid_cast<const ColumnUInt8 *>(input_cols[input_cols.size() - 1]->getPtr().get());
    const auto * hash_array = &hash_col->getData()[0];
    for (size_t i = 0; i < used_cols.size(); ++i)
    {
        filted_cols[i] = used_cols[i]->cloneEmpty();
        #if 0
        if (filted_cols[i]->getDataType() == TypeIndex::Int64)
        {
            auto & pod_array = typeid_cast<ColumnInt64 *>(filted_cols[i]->getPtr().get())->getData();
            pod_array.resize(num_rows);
            const auto & src_pod_array = typeid_cast<const ColumnInt64 *>(used_cols[i]->getPtr().get())->getData();
            size_t n = 0;
            for (size_t r = 0; r < num_rows; ++r)
            {
                if (hash_array[i] != id)
                    continue;
                pod_array[n] = src_pod_array[r];
                n += 1;
            }
            pod_array.resize(n);
        }
        else
        #endif
        {
            filted_cols[i]->reserve(reserve_size);

            for (size_t r = 0; r < num_rows;)
            {
                size_t start = r;
                while (hash_array[r] == id && r < num_rows)
                {
                    r += 1;
                }
                if (r == start)
                {
                    r += 1;
                    continue;
                }
                else
                {
                    filted_cols[i]->insertRangeFrom(*used_cols[i], start, r - start);
                }
            }
        }
    }

    num_rows = filted_cols[0]->size();
    return Chunk(std::move(filted_cols), num_rows);
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

static Block getOutputHeaderForBuildAggregatingKeysHashColumnTransform(const Block & header_)
{
    ColumnsWithTypeAndName cols_with_name_type = header_.getColumnsWithTypeAndName();
    auto hash_col = ColumnUInt32::create(0, 0);
    auto data_type = std::make_shared<DataTypeUInt32>();
    ColumnWithTypeAndName named_hash_col(std::move(hash_col), data_type, "__aggregating_keys_hash");
    cols_with_name_type.emplace_back(std::move(named_hash_col));
    return Block(cols_with_name_type);
}

BuildAggregatingKeysHashColumnTransform::BuildAggregatingKeysHashColumnTransform(
    const Block & header_, const std::vector<size_t> & hash_columns_, size_t num_streams_)
    : IProcessor({header_}, {getOutputHeaderForBuildAggregatingKeysHashColumnTransform(header_)})
    , header(header_)
    , hash_columns(hash_columns_)
    , num_streams(num_streams_)
{
    output_header = getOutputHeaderForBuildAggregatingKeysHashColumnTransform(header_);
    hash_column_name = output_header.getByPosition(output_header.columns() - 1).name;
}

IProcessor::Status BuildAggregatingKeysHashColumnTransform::prepare()
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
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }
    input.setNeeded();
    if (!input.hasData())
    {
        return Status::NeedData;
    }
    output_chunk = input.pull();
    has_input = true;
    return Status::Ready;
}

void BuildAggregatingKeysHashColumnTransform::work()
{
    if (has_input)
    {
        Block original_block = header.cloneWithColumns(output_chunk.detachColumns());
        size_t num_rows = original_block.rows();
        WeakHash32 hash(num_rows);
        for (const auto col_index : hash_columns)
        {
            const auto & key_col = original_block.getByPosition(col_index).column->convertToFullColumnIfConst();
            const auto & key_col_no_lc = recursiveRemoveLowCardinality(recursiveRemoveSparse(key_col));
            key_col_no_lc->updateWeakHash32(hash);
        }

        auto hash_col = ColumnUInt8::create(num_rows, 0);
        auto & hash_data = hash.getData();
        auto & col_data = hash_col->getData();
        auto * col_data_ptr = &col_data[0];
        auto * hash_data_ptr = &hash_data[0];
        for (size_t i = 0; i < num_rows; ++i)
        {
            col_data_ptr[i] = hash_data_ptr[i] % num_streams;
        }

        ColumnWithTypeAndName named_hash_col(std::move(hash_col), std::make_shared<DataTypeUInt32>(), hash_column_name);
        original_block.insert(named_hash_col);

        output_chunk = Chunk(original_block.getColumns(), num_rows);

        has_input = false;
        has_output = true;
    }
}
}
