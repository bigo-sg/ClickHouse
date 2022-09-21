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

#define WRITE_DECIMAL_COLUMN(TYPE, PRIME_TYPE, GETTER) \
    const auto * type_col = checkAndGetColumn<ColumnDecimal<TYPE>>(*nested_col); \
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

#define WRITE_COLUMN_WITH_BACKING_DATA(COL_TYPE) \
    const auto * type_col = checkAndGetColumn<COL_TYPE>(*nested_col); \
    for (auto i = 0; i < num_rows; i++) \
    { \
        bool is_null = nullable_column && nullable_column->isNullAt(i); \
        if (is_null) \
        { \
            setNullAt(buffer_address, offsets[i], field_offset, col_index); \
        } \
        else \
        { \
            StringRef value = type_col->getDataAt(i); \
            memcpy(buffer_address + offsets[i] + buffer_cursor[i], value.data, value.size); \
            int64_t offset_and_size = (buffer_cursor[i] << 32) | value.size; \
            memcpy(buffer_address + offsets[i] + field_offset, &offset_and_size, sizeof(int64_t)); \
            buffer_cursor[i] += value.size; \
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
    unsigned char * buffer_address,
    int64_t field_offset,
    ColumnWithTypeAndName & col,
    int32_t col_index,
    int64_t num_rows,
    const std::vector<int64_t> & offsets,
    std::vector<int64_t> & buffer_cursor)
{
    ColumnPtr nested_col = col.column;
    const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*col.column);
    if (nullable_column)
        nested_col = nullable_column->getNestedColumnPtr();

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
        WRITE_VECTOR_COLUMN(UInt32, int32_t, getInt);
    }
    else if (which.isString())
    {
        WRITE_COLUMN_WITH_BACKING_DATA(ColumnString);
    }
    else if (which.isDecimal())
    {
        if (which.isDecimal32())
        {
            WRITE_DECIMAL_COLUMN(Decimal32, int32_t, getInt);
        }
        else if (which.isDecimal64())
        {
            WRITE_DECIMAL_COLUMN(Decimal64, int64_t, getInt);
        }
        else if (which.isDecimal128())
        {
            WRITE_COLUMN_WITH_BACKING_DATA(ColumnDecimal<Decimal128>);
        }
        else
        {
            WRITE_COLUMN_WITH_BACKING_DATA(ColumnDecimal<Decimal256>);
        }
    }
    else if (which.isArray())
    {
        /// TODO 
    }
    else if (which.isMap())
    {
    }
    else if (which.isTuple())
    {
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support type {} convert from ch to spark" ,magic_enum::enum_name(nested_col->getDataType()));
}

SparkRowInfo::SparkRowInfo(DB::Block & block)
    : num_rows(block.rows())
    , num_cols(block.columns())
    , null_bitset_width_in_bytes(calculateBitSetWidthInBytes(num_cols))
    , total_bytes(0)
    , offsets(num_rows, 0)
    , lengths(num_rows, 0)
    , buffer_cursor(num_rows, 0)
    , buffer_address(nullptr)
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


int64_t SparkRowInfo::getFieldOffset(int32_t col_idx) const
{
    return null_bitset_width_in_bytes + 8L * col_idx;
}

int64_t SparkRowInfo::getNullBitsetWidthInBytes() const
{
    return null_bitset_width_in_bytes;
}

void SparkRowInfo::setNullBitsetWidthInBytes(int64_t null_bitset_width_in_bytes_)
{
    null_bitset_width_in_bytes = null_bitset_width_in_bytes_;
}

int64_t SparkRowInfo::getNumCols() const
{
    return num_cols;
}

void SparkRowInfo::setNumCols(int64_t num_cols_)
{
    num_cols = num_cols_;
}

int64_t SparkRowInfo::getNumRows() const
{
    return num_rows;
}

void SparkRowInfo::setNumRows(int64_t num_rows_)
{
    num_rows = num_rows_;
}

unsigned char * SparkRowInfo::getBufferAddress() const
{
    return buffer_address;
}

void SparkRowInfo::setBufferAddress(unsigned char * buffer_address_)
{
    buffer_address = buffer_address_;
}

const std::vector<int64_t> & SparkRowInfo::getOffsets() const
{
    return offsets;
}

const std::vector<int64_t> & SparkRowInfo::getLengths() const
{
    return lengths;
}

std::vector<int64_t> & SparkRowInfo::getBufferCursor()
{
    return buffer_cursor;
}

int64_t SparkRowInfo::getTotalBytes() const
{
    return total_bytes;
}

std::unique_ptr<SparkRowInfo> CHColumnToSparkRow::convertCHColumnToSparkRow(Block & block)
{
    if (!block.rows() || !block.columns())
        return {};

    std::unique_ptr<SparkRowInfo> spark_row_info = std::make_unique<SparkRowInfo>(block);
    spark_row_info->setBufferAddress(reinterpret_cast<unsigned char *>(alloc(spark_row_info->getTotalBytes())));
    memset(spark_row_info->getBufferAddress(), 0, spark_row_info->getTotalBytes());
    for (auto col_idx = 0; col_idx < spark_row_info->getNumCols(); col_idx++)
    {
        auto col = block.getByPosition(col_idx);
        int64_t field_offset = spark_row_info->getFieldOffset(col_idx);
        writeValue(
            spark_row_info->getBufferAddress(),
            field_offset,
            col,
            col_idx,
            spark_row_info->getNumRows(),
            spark_row_info->getOffsets(),
            spark_row_info->getBufferCursor());
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
    if (which.isNativeInt() || which.isNativeUInt() || which.isFloat() || which.isDateOrDate32()
        || which.isDateTime64() || which.isDecimal32() || which.isDecimal64())
        return 0;
    
    if (which.isStringOrFixedString())
    {
        const auto & str = field.get<String>();
        return roundNumberOfBytesToNearestWord(str.size());
    }

    if (which.isDecimal128())
        return 16;
    
    /// TODO Spark Decimal is impossible mapping to CH Decimal256 
    if (which.isDecimal256())
        return 32;
    
    if (which.isArray())
    {
        /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing buffer
        const auto & array = field.get<Array>(); /// Array can not be wrapped with Nullable
        const auto num_elems = array.size();
        int64_t res = 8 + calculateBitSetWidthInBytes(num_elems);

        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        const auto & nested_type = array_type->getNestedType();
        res += roundNumberOfBytesToNearestWord(getArrayElementSize(nested_type) * num_elems);

        BackingDataLengthCalculator calculator(nested_type);
        for (size_t i=0; i<array.size(); ++i)
            res += calculator.calculate(array[i]);
        return res;
    }
    
    if (which.isMap())
    {
        /// 内存布局：Length of UnsafeArrayData of key(8B) |  UnsafeArrayData of key | UnsafeArrayData of value 
        int64_t res = 8;

        /// Construct Array of keys and values from Map
        const auto & map = field.get<Map>(); /// Map can not be wrapped with Nullable
        const auto num_keys = map.size();
        auto array_key = Array();
        auto array_val = Array();
        array_key.reserve(num_keys);
        array_val.reserve(num_keys);
        for (size_t i=0; i<num_keys; ++i)
        {
            const auto & pair = map[i].get<DB::Tuple>();
            array_key.push_back(pair[0]);
            array_val.push_back(pair[1]);
        }

        const auto * type_map = typeid_cast<const DB::DataTypeMap *>(type.get());

        const auto & type_key = type_map->getKeyType();
        const auto type_array_key = std::make_shared<DataTypeArray>(type_key);
        BackingDataLengthCalculator calculator_key(type_array_key);
        res += calculator_key.calculate(array_key);

        const auto & type_val = type_map->getValueType();
        const auto type_array_val = std::make_shared<DataTypeArray>(type_val);
        BackingDataLengthCalculator calculator_val(type_array_val);
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

int64_t BackingDataLengthCalculator::getArrayElementSize(const DataTypePtr & nested_type)
{
    const WhichDataType nested_which(removeNullable(nested_type));
    if (nested_which.isUInt8() || nested_which.isInt8())
        return 1;
    else if (nested_which.isUInt16() || nested_which.isInt16() || nested_which.isDate())
        return 2;
    else if (
        nested_which.isUInt32() || nested_which.isInt32() || nested_which.isFloat32() || nested_which.isDate32()
        || nested_which.isDecimal32())
        return 4;
    else if (
        nested_which.isUInt64() || nested_which.isInt64() || nested_which.isFloat64() || nested_which.isDateTime64()
        || nested_which.isDecimal64())
        return 8;
    else
        return 8;
}

bool BackingDataLengthCalculator::isFixedLengthDataType(const DB::DataTypePtr & nested_type)
{
    const WhichDataType nested_which(removeNullable(nested_type));
    if (nested_which.isUInt8() || nested_which.isInt8())
        return true;
    else if (nested_which.isUInt16() || nested_which.isInt16() || nested_which.isDate())
        return true;
    else if (
        nested_which.isUInt32() || nested_which.isInt32() || nested_which.isFloat32() || nested_which.isDate32()
        || nested_which.isDecimal32())
        return true;
    else if (
        nested_which.isUInt64() || nested_which.isInt64() || nested_which.isFloat64() || nested_which.isDateTime64()
        || nested_which.isDecimal64())
        return true;
    else
        return false;
}


VariableLengthDataWriter::VariableLengthDataWriter(
    const DB::DataTypePtr & type_,
    unsigned char * buffer_address_,
    const std::vector<int64_t> & offsets_,
    std::vector<int64_t> & buffer_cursor_)
    : type(type_), buffer_address(buffer_address_), offsets(offsets_), buffer_cursor(buffer_cursor_)
{
    assert(type);
    assert(buffer_address);
    assert(!offsets.empty());
    assert(!buffer_cursor.empty());
    assert(offsets.size() == buffer_cursor.size());

    if (BackingDataLengthCalculator::isFixedLengthDataType(type))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataWriter doesn't support type {}", type->getName());
}

int64_t VariableLengthDataWriter::writeArray(size_t row_idx, const DB::Array & array)
{
    /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing data 
    const auto & offset = offsets[row_idx];
    auto & cursor = buffer_cursor[row_idx];
    const auto num_elems = array.size();
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    const auto & nested_type = array_type->getNestedType();

    /// Write numElements(8B)
    const auto start = cursor;
    memcpy(buffer_address + offset + cursor, &num_elems, 8);
    cursor += 8;
    if (num_elems == 0)
        return getOffsetAndSize(start, 8);

    /// Skip null_bitmap(already reset to zero)
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_elems);
    cursor += len_null_bitmap;

    /// Skip values(already reset to zero)
    const auto elem_size = BackingDataLengthCalculator::getArrayElementSize(nested_type);
    const auto len_values = roundNumberOfBytesToNearestWord(elem_size * num_elems);
    cursor += len_values;

    if (BackingDataLengthCalculator::isFixedLengthDataType(nested_type))
    {
        /// If nested type is fixed-length data type, update null_bitmap and values in place
        FixedLengthDataWriter writer(nested_type, buffer_address);
        for (size_t i = 0; i < num_elems; ++i)
        {
            const auto & elem = array[i];
            if (elem.isNull())
                bitSet(buffer_address + offset + start + 8, i);
            else
                writer.write(elem, start + 8 + len_null_bitmap + i * elem_size, true);
        }
    }
    else
    {
        /// If nested type is not fixed-length data type, update null_bitmap in place 
        /// And append values in backing data recursively
        VariableLengthDataWriter writer(nested_type, buffer_address, offsets, buffer_cursor);
        for (size_t i = 0; i < num_elems; ++i)
        {
            const auto & elem = array[i];
            if (elem.isNull())
                bitSet(buffer_address + offset + start + 8, i);
            else
                writer.write(row_idx, elem);
        }
    }
    return getOffsetAndSize(cursor, cursor - start);
}

int64_t VariableLengthDataWriter::writeMap(size_t row_idx, const DB::Map & map)
{
    /// 内存布局：Length of UnsafeArrayData of key(8B) |  UnsafeArrayData of key | UnsafeArrayData of value 
    const auto & offset = offsets[row_idx];
    auto & cursor = buffer_cursor[row_idx];

    /// Skip length of UnsafeArrayData of key(8B)
    const auto start = cursor;
    cursor += 8;
    
    /// If Map is empty, return in advance
    const auto num_pairs = map.size();
    if (num_pairs == 0)
        return getOffsetAndSize(start, 8);

    /// Construct array of keys and array of values from map
    auto array_key = Array();
    auto array_val = Array();
    array_key.reserve(num_pairs);
    array_val.reserve(num_pairs);
    for (size_t i=0; i<num_pairs; ++i)
    {
        const auto & pair = map[i].get<DB::Tuple>();
        array_key.push_back(pair[0]);
        array_val.push_back(pair[1]);
    }

    const auto * type_map = typeid_cast<const DB::DataTypeMap *>(type.get());

    /// Append UnsafeArrayData of key
    const auto & type_key = type_map->getKeyType();
    const auto type_array_key = std::make_shared<DataTypeArray>(type_key);
    VariableLengthDataWriter key_writer(type_array_key, buffer_address, offsets, buffer_cursor);
    const auto array_key_size = extractSize(key_writer.write(row_idx, array_key));

    /// Fill length of UnsafeArrayData of key
    memcpy(buffer_address + offset + start, &array_key_size, 8);

    /// Append UnsafeArrayData of value
    const auto & type_val = type_map->getValueType();
    const auto type_array_val = std::make_shared<DataTypeArray>(type_val);
    VariableLengthDataWriter val_writer(type_array_key, buffer_address, offsets, buffer_cursor);
    val_writer.write(row_idx, array_val);
    return getOffsetAndSize(start, cursor - start);
}

int64_t VariableLengthDataWriter::writeStruct(size_t row_idx, const DB::Tuple & tuple)
{
    /// 内存布局：null_bitmap(字节数与字段数成正比) | values(num_fields * 8B) | backing data 
    const auto & offset = offsets[row_idx];
    auto & cursor = buffer_cursor[row_idx];
    const auto start = cursor;

    /// Skip null_bitmap
    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
    const auto & type_fields = type_tuple->getElements();
    const auto num_fields = type_fields.size();
    if (num_fields == 0)
        return getOffsetAndSize(start, 0);
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_fields);
    cursor += len_null_bitmap;

    /// Skip values
    cursor += num_fields * 8;

    /// If field type is fixed-length, fill field value in values region
    /// else append it to backing data region, and update offset_and_size in values region
    for (size_t i = 0; i < num_fields; ++i)
    {
        const auto & field_value = tuple[i];
        const auto & field_type = type_fields[i];
        if (field_value.isNull())
        {
            bitSet(buffer_address + offset + start, i);
            continue;
        }

        if (BackingDataLengthCalculator::isFixedLengthDataType(field_type))
        {
            FixedLengthDataWriter writer(field_type, buffer_address);
            writer.write(field_value, start + len_null_bitmap + i * 8, false);
        }
        else
        {
            VariableLengthDataWriter writer(field_type, buffer_address, offsets, buffer_cursor);
            const auto offset_and_size = writer.write(row_idx, field_value);
            memcpy(buffer_address + offset + start + len_null_bitmap + 8 * i, &offset_and_size, 8);
        }
    }
    return getOffsetAndSize(start, cursor - start);
}

int64_t VariableLengthDataWriter::write(size_t row_idx, const DB::Field &field)
{
    assert(row_idx < offsets.size());

    if (field.isNull())
        return 0;
    
    const WhichDataType which(removeNullable(type));
    if (which.isStringOrFixedString())
    {
        const auto & str = field.get<String>();
        return writeUnalignedBytes(row_idx, str.data(), str.size());
    }

    if (which.isDecimal128())
    {
        const auto & decimal = field.get<DecimalField<Decimal128>>();
        const auto value = decimal.getValue();
        return writeUnalignedBytes(row_idx, &value, sizeof(Decimal128));
    }

    if (which.isDecimal256())
    {
        const auto & decimal = field.get<DecimalField<Decimal256>>();
        const auto value = decimal.getValue();
        return writeUnalignedBytes(row_idx, &value, sizeof(Decimal256));
    }

    if (which.isArray())
    {
        const auto & array = field.get<Array>();
        return writeArray(row_idx, array);
    }
    
    if (which.isMap())
    {
        const auto & map = field.get<Map>();
        return writeMap(row_idx, map);
    }

    if (which.isTuple())
    {
        const auto & tuple = field.get<Tuple>();
        return writeStruct(row_idx, tuple);
    }

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "Doesn't support type {} for BackingDataWriter", type->getName());
}

int64_t VariableLengthDataWriter::getOffsetAndSize(int64_t cursor, int64_t size)
{
    return (cursor << 32) | size;
}

int64_t VariableLengthDataWriter::extractOffset(int64_t offset_and_size)
{
    return offset_and_size >> 32;
}

int64_t VariableLengthDataWriter::extractSize(int64_t offset_and_size)
{
    return offset_and_size & 0xffffffff;
}

int64_t VariableLengthDataWriter::writeUnalignedBytes(size_t row_idx, const void * src, size_t size)
{
    memcpy(buffer_address + offsets[row_idx] + buffer_cursor[row_idx], src, size);
    auto res = getOffsetAndSize(buffer_cursor[row_idx], size);
    buffer_cursor[row_idx] += roundNumberOfBytesToNearestWord(size);
    return res;
}


FixedLengthDataWriter::FixedLengthDataWriter(const DB::DataTypePtr & type_, unsigned char * buffer_address_)
    : type(type_), which(removeNullable(type)), buffer_address(buffer_address_)
{
    if (!BackingDataLengthCalculator::isFixedLengthDataType(type))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "FixedLengthWriter doesn't support type {}", type->getName());
}

void FixedLengthDataWriter::write(const DB::Field & field, int64_t offset, bool is_array_element)
{
    /// Skip null value
    if (field.isNull())
        return;

    if (which.isUInt8())
    {
        const auto & value = field.get<UInt8>();
        memcpy(buffer_address + offset, &value, is_array_element ? 1 : 8);
    }
    else if (which.isUInt16() || which.isDate())
    {
        const auto & value = field.get<UInt16>();
        memcpy(buffer_address + offset, &value, is_array_element ? 2 : 8);
    }
    else if (which.isUInt32() || which.isDate32())
    {
        const auto & value = field.get<UInt32>();
        memcpy(buffer_address + offset, &value, is_array_element ? 4 : 8);
    }
    else if (which.isUInt64())
    {
        const auto & value = field.get<UInt64>();
        memcpy(buffer_address + offset, &value, 8);
    }
    else if (which.isInt8())
    {
        const auto & value = field.get<Int8>();
        memcpy(buffer_address + offset, &value, is_array_element ? 1 : 8);
    }
    else if (which.isInt16())
    {
        const auto & value = field.get<Int16>();
        memcpy(buffer_address + offset, &value, is_array_element ? 2 : 8);
    }
    else if (which.isInt32())
    {
        const auto & value = field.get<Int32>();
        memcpy(buffer_address + offset, &value, is_array_element ? 4 : 8);
    }
    else if (which.isInt64())
    {
        const auto & value = field.get<Int64>();
        memcpy(buffer_address + offset, &value, 8);
    }
    else if (which.isFloat32())
    {
        const auto value = Float32(field.get<Float32>());
        memcpy(buffer_address + offset, &value, is_array_element ? 4 : 8);
    }
    else if (which.isFloat64())
    {
        const auto value = Float32(field.get<Float64>());
        memcpy(buffer_address + offset, &value, 8);
    }
    else if (which.isDecimal32())
    {
        const auto & value = field.get<Decimal32>();
        auto decimal = value.getValue();
        memcpy(buffer_address + offset, &decimal, is_array_element ? 4 : 8);
    }
    else if (which.isDecimal64() || which.isDateTime64())
    {
        const auto & value = field.get<Decimal64>();
        auto decimal = value.getValue();
        memcpy(buffer_address + offset, &decimal, 8);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "FixedLengthWriter doesn't support type {}", type->getName());
}

}
