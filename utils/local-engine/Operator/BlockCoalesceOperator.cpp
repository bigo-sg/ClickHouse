#include "BlockCoalesceOperator.h"
#include <memory>
#include <stdexcept>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Parser/SerializedPlanParser.h>
#include <Poco/StringTokenizer.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{

BlockCoalesceOperator::BlockCoalesceOperator(size_t buf_size_, const String & schema_) : buf_size(buf_size_), schema(schema_)
{
    substrait::NamedStruct named_struct;
    named_struct.ParseFromString(schema);
    LOG_ERROR(&Poco::Logger::get("BlockCoalesceOperator"), "xxx schema: {}", named_struct.DebugString());
    auto header = SerializedPlanParser::parseNameStruct(named_struct);
    block_buffer = std::make_unique<ColumnsBuffer>(header);
}
void BlockCoalesceOperator::mergeBlock(DB::Block & block)
{
    block_buffer->add(block, 0, block.rows());
}
bool BlockCoalesceOperator::isFull()
{
    return block_buffer->size() >= buf_size;
}
DB::Block* BlockCoalesceOperator::releaseBlock()
{
    clearCache();
    cached_block = new DB::Block(block_buffer->releaseColumns());
    return cached_block;
}
BlockCoalesceOperator::~BlockCoalesceOperator()
{
    clearCache();
}
void BlockCoalesceOperator::clearCache()
{
    if (cached_block)
    {
        delete cached_block;
        cached_block = nullptr;
    }
}
}

