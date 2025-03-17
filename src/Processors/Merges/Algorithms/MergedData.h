#pragma once

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/TypeId.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>

namespace DB
{

template <TypeIndex type_index>
struct ColumnInserter
{
    template <typename T>
    explicit ColumnInserter(T value)
    {
        static_assert(std::is_same_v<T, uint8_t>, "Argument must be an uint8_t");
    }

#define DISPATCH_TYPE_INDEXES(M1, M2) \
    M1(UInt8) \
    M1(UInt16) \
    M1(UInt32) \
    M1(UInt64) \
    M1(Int8) \
    M1(Int16) \
    M1(Int32) \
    M1(Int64) \
    M1(Float32) \
    M1(Float64) \
    M1(String) \
    M1(FixedString) \
    M1(BFloat16) \
    M1(UInt128) \
    M1(UInt256) \
    M1(Int128) \
    M1(Int256) \
    M1(UUID) \
    M1(IPv4) \
    M1(IPv6) \
    M2(DateTime64, ColumnDecimal<DateTime64>) \
    M2(Decimal32, ColumnDecimal<Decimal32>) \
    M2(Decimal64, ColumnDecimal<Decimal64>) \
    M2(Decimal128, ColumnDecimal<Decimal128>) \
    M2(Decimal256, ColumnDecimal<Decimal256>)


    ALWAYS_INLINE void insertFrom(IColumn & dst, const IColumn & src, size_t n) const
    {
#define INSERT_FROM_WITH_COLUMN_TYPE(TYPE_INDEX, COLUMN_TYPE) \
    if constexpr (type_index == TypeIndex::TYPE_INDEX) \
    { \
        auto * concrete_dst = typeid_cast<COLUMN_TYPE *>(&dst); \
        if (concrete_dst) \
            concrete_dst->insertFrom(src, n); \
        else \
            dst.insertFrom(src, n); \
        return; \
    }

#define INSERT_FROM(TYPE_INDEX) INSERT_FROM_WITH_COLUMN_TYPE(TYPE_INDEX, Column##TYPE_INDEX)

        DISPATCH_TYPE_INDEXES(INSERT_FROM, INSERT_FROM_WITH_COLUMN_TYPE)
        dst.insertFrom(src, n);

#undef INSERT_FROM
#undef INSERT_FROM_WITH_COLUMN_TYPE
    }

    ALWAYS_INLINE void insertRangeFrom(IColumn & dst, const IColumn & src, size_t start, size_t length) const
    {
#define INSERT_RANGE_FROM_WITH_COLUMN_TYPE(TYPE_INDEX, COLUMN_TYPE) \
    if constexpr (type_index == TypeIndex::TYPE_INDEX) \
    { \
        auto * concrete_dst = typeid_cast<COLUMN_TYPE *>(&dst); \
        if (concrete_dst) \
            concrete_dst->insertRangeFrom(src, start, length); \
        else \
            dst.insertRangeFrom(src, start, length); \
        return; \
    }

#define INSERT_RANGE_FROM(TYPE_INDEX) INSERT_RANGE_FROM_WITH_COLUMN_TYPE(TYPE_INDEX, Column##TYPE_INDEX)

        DISPATCH_TYPE_INDEXES(INSERT_RANGE_FROM, INSERT_RANGE_FROM_WITH_COLUMN_TYPE)
        dst.insertRangeFrom(src, start, length);

#undef INSERT_RANGE_FROM
#undef INSERT_RANGE_FROM_WITH_COLUMN_TYPE
    }
#undef DISPATCH_TYPE_INDEXES
};

template <typename T>
ColumnInserter(T) -> ColumnInserter<static_cast<TypeIndex>(T{})>;

class Block;

/// Class which represents current merging chunk of data.
/// Also it calculates the number of merged rows and other profile info.
class MergedData
{
public:
    explicit MergedData(bool use_average_block_size_, UInt64 max_block_size_, UInt64 max_block_size_bytes_)
        : max_block_size(max_block_size_)
        , max_block_size_bytes(max_block_size_bytes_)
        , use_average_block_size(use_average_block_size_)
    {
    }

    virtual void initialize(const Block & header, const IMergingAlgorithm::Inputs & inputs);

    /// Pull will be called at next prepare call.
    void flush() { need_flush = true; }

    void insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size);

    void insertRows(const ColumnRawPtrs & raw_columns, size_t start_index, size_t length, size_t block_size);

    void insertChunk(Chunk && chunk, size_t rows_size);

    Chunk pull();

    bool hasEnoughRows() const;

    UInt64 mergedRows() const { return merged_rows; }
    UInt64 totalMergedRows() const { return total_merged_rows; }
    UInt64 totalChunks() const { return total_chunks; }
    UInt64 totalAllocatedBytes() const { return total_allocated_bytes; }
    UInt64 maxBlockSize() const { return max_block_size; }

    IMergingAlgorithm::MergedStats getMergedStats() const
    {
        return {.bytes = total_allocated_bytes, .rows = total_merged_rows, .blocks = total_chunks};
    }

    virtual ~MergedData() = default;

protected:
    MutableColumns columns;

    UInt64 sum_blocks_granularity = 0;
    UInt64 merged_rows = 0;
    UInt64 total_merged_rows = 0;
    UInt64 total_chunks = 0;
    UInt64 total_allocated_bytes = 0;

    const UInt64 max_block_size = 0;
    const UInt64 max_block_size_bytes = 0;
    const bool use_average_block_size = false;

    bool need_flush = false;
};

}
