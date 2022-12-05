#include <Interpreters/createBlockSelector.h>
#include <Processors/Port.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <Common/WeakHash.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

static OutputPorts buildShuffleOutports(size_t num_streams, const Block & header)
{
    OutputPorts outports;
    for (size_t i = 0; i < num_streams; ++i)
    {
        OutputPort outport(header);
        outports.push_back(outport);
    }
    return outports;
}
InnerShuffleTransform::InnerShuffleTransform(size_t num_streams_, const Block & header_, const std::vector<size_t> & hash_columns_)
    : IProcessor({header_}, buildShuffleOutports(num_streams_, header_))
    , num_streams(num_streams_)
    , header(header_)
    , hash_columns(hash_columns_)
{
}

IProcessor::Status InnerShuffleTransform::prepare()
{
    bool all_output_finished = true;
    for (auto & output : outputs)
    {
        if (!output.isFinished())
        {
            all_output_finished = false;
            break;
        }
    }
    if (all_output_finished)
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    for (auto & output : outputs)
    {
        if (!output.canPush())
        {
            for (auto & input : inputs)
            {
                input.setNotNeeded();
            }
            return Status::PortFull;
        }
    }

    if (has_output)
    {
        auto output_it = outputs.begin();
        auto chunk_it = output_chunks.begin();
        for (; output_it != outputs.end(); ++output_it)
        {
            if(chunk_it->getNumRows())
            {
                if (output_it->isFinished())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Output port is finished, cannot push new chunks into it");
                output_it->push(std::move(*chunk_it));
            }
            chunk_it++;
        }
        output_chunks.clear();
        has_output = false;
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;
    
    auto & input = inputs.front();
    if (input.isFinished())
    {
        for (auto & output : outputs)
        {
            output.finish();
        }
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void InnerShuffleTransform::work()
{
    if (!has_input)
    {
        return;
    }
    Block block = header.cloneWithColumns(input_chunk.detachColumns());
    size_t num_rows = block.rows();
    WeakHash32 hash(num_rows);
    for (const auto col_index : hash_columns)
    {
        block.getByPosition(col_index).column->updateWeakHash32(hash);
    }

    IColumn::Selector selector(num_rows);
    const auto & hash_data = hash.getData();
    for (size_t i = 0; i < num_rows; ++i)
    {
        selector[i] = hash_data[i] % num_streams;
    }

    Blocks result_blocks;
    for (size_t i = 0; i < num_streams; ++i)
    {
        result_blocks.emplace_back(header.cloneEmpty());
    }

    for (size_t i = 0, num_cols = header.columns(); i < num_cols; ++i)
    {
        auto shuffled_columms = block.getByPosition(i).column->scatter(num_streams, selector);
        for (size_t block_index = 0; block_index < num_streams; ++block_index)
        {
            result_blocks[block_index].getByPosition(i).column = std::move(shuffled_columms[block_index]);
        }
    }
    for (auto & result_block : result_blocks)
    {
        output_chunks.emplace_back(Chunk(result_block.getColumns(), result_block.rows()));
    }

    has_output = true;
    has_input = false;
}

}
