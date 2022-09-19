#include "CHColumnToSparkRow.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

#define WRITE_VECTOR_COLUMN(TYPE, PRIME_TYPE, GETTER) \
    const auto * type_col = checkAndGetColumn<ColumnVector<TYPE>>(*nested_col); \
    for (auto i = 0; i < num_rows; i++) \
    { \
        bool is_null = nullable_column && nullable_column->isNullAt(i); \
        if (is_null) \
        { \
            setNullAt(buffer_address, offsets[i], field_offset, col_index); \
        } \
        else \
        { \
            auto * pointer = reinterpret_cast<PRIME_TYPE *>(buffer_address + offsets[i] + field_offset); \
            pointer[0] = type_col->GETTER(i);\
        } \
    }

namespace local_engine
{
using namespace DB;

int64_t calculateBitSetWidthInBytes(int32_t num_fields)
{
    return ((num_fields + 63) / 64) * 8;
}

static int64_t calculatedFixeSizePerRow(int64_t num_cols)
{
    return calculateBitSetWidthInBytes(num_cols) + num_cols * 8;
}

static int64_t roundNumberOfBytesToNearestWord(int64_t num_bytes)
{
    auto remainder = num_bytes & 0x07; // This is equivalent to `numBytes % 8`
    return num_bytes + ((8 - remainder) & 0x7);
}

int64_t getFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index)
{
    return nullBitsetWidthInBytes + 8L * index;
}

void bitSet(uint8_t * buffer_address, int32_t index)
{
    int64_t mask = 1L << (index & 0x3f); // mod 64 and shift
    int64_t word_offset = (index >> 6) * 8;
    int64_t word;
    memcpy(&word, buffer_address + word_offset, sizeof(int64_t));
    int64_t value = word | mask;
    memcpy(buffer_address + word_offset, &value, sizeof(int64_t));
}

void setNullAt(uint8_t * buffer_address, int64_t row_offset, int64_t field_offset, int32_t col_index)
{
    bitSet(buffer_address + row_offset, col_index);
    // set the value to 0
    memset(buffer_address + row_offset + field_offset, 0, sizeof(int64_t));
}

void writeValue(
    uint8_t * buffer_address,
    int64_t field_offset,
    ColumnWithTypeAndName & col,
    int32_t col_index,
    int64_t num_rows,
    std::vector<int64_t> & offsets,
    std::vector<int64_t> & buffer_cursor)
{
    ColumnPtr nested_col = col.column;
    const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*col.column);
    if (nullable_column)
    {
        nested_col = nullable_column->getNestedColumnPtr();
    }
    nested_col = nested_col->convertToFullColumnIfConst();
    WhichDataType which(nested_col->getDataType());
    if (which.isUInt8())
    {
        WRITE_VECTOR_COLUMN(UInt8, uint8_t, getInt)
    }
    else if (which.isInt8())
    {
        WRITE_VECTOR_COLUMN(Int8, int8_t, getInt)
    }
    else if (which.isInt16())
    {
        WRITE_VECTOR_COLUMN(Int16, int16_t, getInt)
    }
    else if (which.isUInt16())
    {
        WRITE_VECTOR_COLUMN(UInt16, uint16_t , get64)
    }
    else if (which.isInt32())
    {
        WRITE_VECTOR_COLUMN(Int32, int32_t, getInt)
    }
    else if (which.isInt64())
    {
        WRITE_VECTOR_COLUMN(Int64, int64_t, getInt)
    }
    else if (which.isUInt64())
    {
        WRITE_VECTOR_COLUMN(UInt64, int64_t, get64)
    }
    else if (which.isFloat32())
    {
        WRITE_VECTOR_COLUMN(Float32, float_t, getFloat32)
    }
    else if (which.isFloat64())
    {
        WRITE_VECTOR_COLUMN(Float64, double_t, getFloat64)
    }
    else if (which.isDate())
    {
        WRITE_VECTOR_COLUMN(UInt16, uint16_t, get64)
    }
    else if (which.isDate32())
    {
        WRITE_VECTOR_COLUMN(UInt32, uint32_t, get64)
    }
    else if (which.isString())
    {
        const auto * string_col = checkAndGetColumn<ColumnString>(*nested_col);
        for (auto i = 0; i < num_rows; i++)
        {
            bool is_null = nullable_column && nullable_column->isNullAt(i);
            if (is_null)
            {
                setNullAt(buffer_address, offsets[i], field_offset, col_index);
            }
            else
            {
                StringRef string_value = string_col->getDataAt(i);
                // write the variable value
                memcpy(buffer_address + offsets[i] + buffer_cursor[i], string_value.data, string_value.size);
                // write the offset and size
                int64_t offset_and_size = (buffer_cursor[i] << 32) | string_value.size;
                memcpy(buffer_address + offsets[i] + field_offset, &offset_and_size, sizeof(int64_t));
                buffer_cursor[i] += string_value.size;
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support type {} convert from ch to spark" ,magic_enum::enum_name(nested_col->getDataType()));
    }
}

SparkRowInfo::SparkRowInfo(DB::Block & block)
    : num_rows(block.rows())
    , num_cols(block.columns())
    , null_bitset_width_in_bytes(calculateBitSetWidthInBytes(num_cols))
    , total_bytes(0)
    , offsets(num_rows, 0)
    , lengths(num_rows, 0)
    , buffer_cursor(num_rows, 0)
{
    int64_t fixed_size_per_row = calculatedFixeSizePerRow(num_cols);

    /// Initialize lengths and buffer_cursor
    for (auto i = 0; i < num_rows; i++)
    {
        lengths[i] = fixed_size_per_row;
        buffer_cursor[i] = fixed_size_per_row;
    }
    for (auto col_idx = 0; col_idx < num_cols; ++col_idx)
    {
        const auto & col = block.getByPosition(col_idx);
        BackingDataLengthCalculator calculator(col.type);
        for (auto row_idx = 0; row_idx < num_rows; ++row_idx)
        {
            const auto field = (*col.column)[row_idx];
            lengths[row_idx] += calculator.calculate(field);
        }
    }

    /// Initialize offsets
    for (auto i=1; i<num_rows; ++i)
        offsets[i] = offsets[i - 1] + lengths[i - 1];
    
    /// Initialize total_bytes
    for (auto i=0; i<num_rows; ++i)
        total_bytes += lengths[i];
}

int64_t local_engine::SparkRowInfo::getNullBitsetWidthInBytes() const
{
    return null_bitset_width_in_bytes;
}

void local_engine::SparkRowInfo::setNullBitsetWidthInBytes(int64_t null_bitset_width_in_bytes_)
{
    null_bitset_width_in_bytes = null_bitset_width_in_bytes_;
}
int64_t local_engine::SparkRowInfo::getNumCols() const
{
    return num_cols;
}
void local_engine::SparkRowInfo::setNumCols(int64_t num_cols_)
{
    num_cols = num_cols_;
}

int64_t local_engine::SparkRowInfo::getNumRows() const
{
    return num_rows;
}

void local_engine::SparkRowInfo::setNumRows(int64_t num_rows_)
{
    num_rows = num_rows_;
}

unsigned char * local_engine::SparkRowInfo::getBufferAddress() const
{
    return buffer_address;
}

void local_engine::SparkRowInfo::setBufferAddress(unsigned char * buffer_address_)
{
    buffer_address = buffer_address_;
}

const std::vector<int64_t> & local_engine::SparkRowInfo::getOffsets() const
{
    return offsets;
}
const std::vector<int64_t> & local_engine::SparkRowInfo::getLengths() const
{
    return lengths;
}

int64_t SparkRowInfo::getTotalBytes() const
{
    return total_bytes;
}

std::unique_ptr<SparkRowInfo> local_engine::CHColumnToSparkRow::convertCHColumnToSparkRow(Block & block)
{
    std::unique_ptr<SparkRowInfo> spark_row_info = std::make_unique<SparkRowInfo>(block);
    spark_row_info->buffer_address = reinterpret_cast<unsigned char *>(alloc(spark_row_info->total_bytes));
    memset(spark_row_info->buffer_address, 0, sizeof(int8_t) * spark_row_info->total_bytes);
    for (auto i = 0; i < spark_row_info->num_cols; i++)
    {
        auto col = block.getByPosition(i);
        int64_t field_offset = getFieldOffset(spark_row_info->null_bitset_width_in_bytes, i);
        writeValue(
            spark_row_info->buffer_address,
            field_offset,
            col,
            i,
            spark_row_info->num_rows,
            spark_row_info->offsets,
            spark_row_info->buffer_cursor);
    }
    return spark_row_info;
}

void CHColumnToSparkRow::freeMem(uint8_t * address, size_t size)
{
    free(address, size);
}

BackingDataLengthCalculator::BackingDataLengthCalculator(const DataTypePtr & type_) : type(type_)
{
}

int64_t BackingDataLengthCalculator::calculate(const Field & field) const
{
    if (field.isNull())
        return 0;

    const WhichDataType which(removeNullable(type));
    if (which.isNothing() || which.isNativeInt() || which.isNativeUInt() || which.isFloat() || which.isEnum() || which.isDateOrDate32()
        || which.isDateTime() || which.isDateTime64())
        return 0;
    
    if (which.isStringOrFixedString())
    {
        const auto & str = field.get<String>();
        return roundNumberOfBytesToNearestWord(str.size());
    }

    if (which.isDecimal())
    {
        if (which.isDecimal32() || which.isDecimal64())
            return 0;
        else if (which.isDecimal128())
            return 16;
        else
            return 32;
    }
    
    if (which.isArray())
    {
        /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing buffer
        const auto & array = field.get<Array>(); /// Array can not be wrapped with Nullable
        const auto num_elems = array.size();
        int64_t res = 8 + calculateBitSetWidthInBytes(num_elems);

        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        const auto & nested_type = array_type->getNestedType();
        const WhichDataType nested_which(removeNullable(nested_type));
        int64_t values_length = 0;
        if (nested_which.isUInt8() || nested_which.isInt8())
            values_length = num_elems;
        else if (nested_which.isUInt16() || nested_which.isInt16())
            values_length = num_elems * 2;
        else if (nested_which.isUInt32() || nested_which.isInt32() || nested_which.isFloat32())
            values_length = num_elems * 4;
        else if (nested_which.isUInt64() || nested_which.isInt64() || nested_which.isFloat64())
            values_length = num_elems * 8;
        else
            values_length = num_elems * 8;
        res += roundNumberOfBytesToNearestWord(values_length);

        BackingDataLengthCalculator calculator(nested_type);
        for (size_t i=0; i<array.size(); ++i)
            res += calculator.calculate(array[i]);
        return res;
    }
    
    if (which.isMap())
    {
        /// 内存布局：numKeys(8B) | null_bitmap(字节数与map长度成正比) |  UnsafeArrayData of key | UnsafeArrayData of value 
        const auto & map = field.get<Map>(); /// Map can not be wrapped with Nullable
        const auto num_keys = map.size();
        int64_t res = 8 + calculateBitSetWidthInBytes(num_keys);
        
        auto array_key = Array();
        auto array_val = Array();
        array_key.reserve(num_keys);
        array_val.reserve(num_keys);
        for (size_t i=0; i<num_keys; ++i)
        {
            const auto & pair = map[i].get<DB::Tuple>();
            array_key.push_back(pair[0]);
            array_key.push_back(pair[1]);
        }

        const auto * type_map = typeid_cast<const DB::DataTypeMap *>(type.get());
        const auto & type_key = type_map->getKeyType();
        const auto & type_val = type_map->getValueType();
        BackingDataLengthCalculator calculator_key(type_key);
        BackingDataLengthCalculator calculator_val(type_val);
        res += calculator_key.calculate(array_key);
        res += calculator_key.calculate(array_val);
        return res;
    }

    if (which.isTuple())
    {
        /// 内存布局：null_bitmap(字节数与字段数成正比) | field1 value(8B) | field2 value(8B) | ... | fieldn value(8B) | backing buffer
        const auto & tuple = field.get<Tuple>(); /// Tuple can not be wrapped with Nullable
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
        const auto & type_fields = type_tuple->getElements();
        const auto num_fields = type_fields.size();
        int64_t res = calculateBitSetWidthInBytes(num_fields) + 8 * num_fields;
        for (size_t i=0; i<num_fields; ++i)
        {
            BackingDataLengthCalculator calculator(type_fields[i]);
            res += calculator.calculate(tuple[i]);
        }
        return res;
    }
    
    throw Exception(ErrorCodes::UNKNOWN_TYPE, "Doesn't support type {} for BackingBufferLengthCalculator", type->getName());
}

}
