#pragma once

#include <memory>
#include <Shuffle/ShuffleSplitter.h>

namespace DB
{
class Block;
}

namespace local_engine
{
class BlockCoalesceOperator
{
public:
    explicit BlockCoalesceOperator(size_t buf_size_, const String & schema_);
    virtual ~BlockCoalesceOperator();
    void mergeBlock(DB::Block & block);
    bool isFull();
    DB::Block* releaseBlock();

private:
    size_t buf_size;
    String schema;
    std::unique_ptr<ColumnsBuffer> block_buffer;
    DB::Block * cached_block = nullptr;

    void clearCache();
};
}


