#pragma once
#include <vector>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Common/Allocator.h>


namespace local_engine
{
int64_t calculateBitSetWidthInBytes(int32_t num_fields);
int64_t roundNumberOfBytesToNearestWord(int64_t num_bytes);
void bitSet(char * bitmap, int32_t index);
bool isBitSet(char * bitmap, int32_t index);

class CHColumnToSparkRow;
class SparkRowToCHColumn;

class SparkRowInfo
{
    friend CHColumnToSparkRow;
    friend SparkRowToCHColumn;

public:
    explicit SparkRowInfo(DB::Block & block);

    int64_t getFieldOffset(int32_t col_idx) const;

    int64_t getNullBitsetWidthInBytes() const;
    void setNullBitsetWidthInBytes(int64_t null_bitset_width_in_bytes_);

    int64_t getNumCols() const;
    void setNumCols(int64_t num_cols_);

    int64_t getNumRows() const;
    void setNumRows(int64_t num_rows_);

    char * getBufferAddress() const;
    void setBufferAddress(char * buffer_address);

    const std::vector<int64_t> & getOffsets() const;
    const std::vector<int64_t> & getLengths() const;
    std::vector<int64_t> & getBufferCursor();
    int64_t getTotalBytes() const;

private:
    int64_t num_rows;
    int64_t num_cols;
    int64_t null_bitset_width_in_bytes;
    int64_t total_bytes;

    std::vector<int64_t> offsets;
    std::vector<int64_t> lengths;
    std::vector<int64_t> buffer_cursor;
    char * buffer_address;
};

using SparkRowInfoPtr = std::unique_ptr<local_engine::SparkRowInfo>;

class CHColumnToSparkRow : private Allocator<false>
{
public:
    std::unique_ptr<SparkRowInfo> convertCHColumnToSparkRow(DB::Block & block);
    void freeMem(char * address, size_t size);
};

/// Return backing data length of values with variable-length type in bytes
class BackingDataLengthCalculator
{
public:
    static constexpr size_t DECIMAL_MAX_INT64_DIGITS = 18;

    explicit BackingDataLengthCalculator(const DB::DataTypePtr & type_);
    virtual ~BackingDataLengthCalculator() = default;

    /// Return length is guranteed to round up to 8
    virtual int64_t calculate(const DB::Field & field) const;

    static int64_t getArrayElementSize(const DB::DataTypePtr & nested_type);

    /// Is CH DataType can be converted to fixed-length data type in Spark?
    static bool isFixedLengthDataType(const DB::DataTypePtr & type);

    /// Is CH DataType can be converted to variable-length data type in Spark?
    static bool isVariableLengthDataType(const DB::DataTypePtr & type);

    static int64_t getOffsetAndSize(int64_t cursor, int64_t size);
    static int64_t extractOffset(int64_t offset_and_size);
    static int64_t extractSize(int64_t offset_and_size);

private:
    const DB::DataTypePtr type;
};

/// Writing variable-length typed values to backing data region of Spark Row
/// User who calls VariableLengthDataWriter is responsible to write offset_and_size
/// returned by VariableLengthDataWriter::write to field value in Spark Row
class VariableLengthDataWriter
{
public:
    VariableLengthDataWriter(
        const DB::DataTypePtr & type_,
        char * buffer_address_,
        const std::vector<int64_t> & offsets_,
        std::vector<int64_t> & buffer_cursor_);

    virtual ~VariableLengthDataWriter() = default;

    /// Write value of variable-length to backing data region of structure(row or array) and return offset and size in backing data region
    /// It's caller's duty to make sure that row fields or array elements are written in order
    /// parent_offset: the starting offset of current structure in which we are updating it's backing data region
    virtual int64_t write(size_t row_idx, const DB::Field & field, int64_t parent_offset);

private:

    int64_t writeUnalignedBytes(size_t row_idx, const void * src, size_t size, int64_t parent_offset);
    int64_t writeArray(size_t row_idx, const DB::Array & array, int64_t parent_offset);
    int64_t writeMap(size_t row_idx, const DB::Map & map, int64_t parent_offset);
    int64_t writeStruct(size_t row_idx, const DB::Tuple & tuple, int64_t parent_offset);

    const DB::DataTypePtr type;

    /// Global buffer of spark rows
    char * const buffer_address;
    /// Offsets of each spark row
    const std::vector<int64_t> & offsets;
    /// Cursors of backing data in each spark row, relative to offsets
    std::vector<int64_t> & buffer_cursor;
};

class FixedLengthDataWriter
{
public:
    explicit FixedLengthDataWriter(const DB::DataTypePtr & type_);
    virtual ~FixedLengthDataWriter() = default;

    /// Write value of fixed-length to values region of structure(struct or array)
    /// It's caller's duty to make sure that struct fields or array elements are written in order
    virtual void write(const DB::Field & field, char * buffer);

private:
    const DB::DataTypePtr & type;
    const DB::WhichDataType which;
};

}
