#pragma once

#include <base/types.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
namespace DB
{
struct ColumnStatMetadata
{
    size_t column_ref = 0;
    size_t rows = 0;
    size_t distinct_count = 0;
    size_t null_count = 0;

    String debugString() const;
};
using ColumnStatMetadataPtr = std::shared_ptr<ColumnStatMetadata>;

struct BlockStatMetadata
{
    std::vector<ColumnStatMetadataPtr> columns_metadata;
};

class BlockStatAnalyzer
{
public:
    explicit BlockStatAnalyzer(const std::vector<Block> & sample_blocks_);
    BlockStatMetadata analyze();
private:
    std::vector<Block> sample_blocks;

    static size_t countNullValue(const ColumnWithTypeAndName & col);
};
}
