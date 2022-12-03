#include "BlockStatAnalyzer.h"
#include <Interpreters/SetVariants.h>
#include <base/types.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include "Common/typeid_cast.h"
#include "Columns/ColumnNullable.h"
#include "Columns/ColumnSparse.h"

namespace DB
{
String ColumnStatMetadata::debugString() const
{
    WriteBufferFromOwnString buffer;
    buffer << "{";
    buffer << "\"column_ref\":" << column_ref;
    buffer << ", \"rows\":" << rows;
    buffer << ", \"distinct_count\":"<<distinct_count;
    buffer << ", \"null_count\":"<<null_count;
    buffer << "}";
    return buffer.str();
}

BlockStatAnalyzer::BlockStatAnalyzer(const std::vector<Block> & sample_blocks_)
    : sample_blocks(sample_blocks_)
{}

template <typename Method>
void buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    const size_t rows,
    Sizes & key_sizes,
    SetVariants & variants)
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        (void)state.emplaceKey(method.data, i, variants.string_pool);
    }
}

BlockStatMetadata BlockStatAnalyzer::analyze()
{
    if (sample_blocks.empty())
        return {};
    auto & first_block = sample_blocks[0];
    BlockStatMetadata block_stat;
    for (size_t i = 0, n = first_block.columns(); i < n; ++i)
    {
        auto column_stat = std::make_shared<ColumnStatMetadata>();
        column_stat->column_ref = i;

        SetVariants value_set;
        Sizes key_sizes;
        ColumnRawPtrs column_ptrs;
        column_ptrs.emplace_back(first_block.getByPosition(i).column.get());
        value_set.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

        for (const auto & block : sample_blocks)
        {
            column_stat->rows += block.rows();
            const auto & col_with_name_type = block.getByPosition(i);
            column_stat->null_count += countNullValue(col_with_name_type);

            ColumnRawPtrs inner_column_ptrs;
            auto current_col = recursiveRemoveSparse(col_with_name_type.column);
            inner_column_ptrs.emplace_back(current_col.get());
            switch (value_set.type)
            {
                case SetVariants::Type::EMPTY:
                    break;
#define M(NAME) \
                case SetVariants::Type::NAME: \
                    buildFilter(*value_set.NAME, inner_column_ptrs, block.rows(),  key_sizes, value_set); \
                    break;
                APPLY_FOR_SET_VARIANTS(M)
#undef M
            }
            column_stat->distinct_count = value_set.getTotalRowCount();
        }

        block_stat.columns_metadata.emplace_back(column_stat);
    }
    return block_stat;
}

size_t BlockStatAnalyzer::countNullValue(const ColumnWithTypeAndName & col)
{
    if (!col.column->isNullable())
        return 0;
    const auto * nullable_col = typeid_cast<const ColumnNullable*>(col.column.get());
    const auto & null_map = nullable_col->getNullMapColumn();
    size_t n = 0;
    for (size_t i = 0; i < null_map.size(); ++i)
    {
        n += (null_map[i] != 0);
    }
    return n;
}
}
