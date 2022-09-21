#include "SparkRowToCHColumn.h"
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

using namespace DB;

namespace local_engine
{
jclass SparkRowToCHColumn::spark_row_interator_class = nullptr;
jmethodID SparkRowToCHColumn::spark_row_interator_hasNext = nullptr;
jmethodID SparkRowToCHColumn::spark_row_interator_next = nullptr;

int64_t getStringColumnTotalSize(int ordinal, SparkRowInfo & spark_row_info)
{
    SparkRowReader reader(spark_row_info.getNumCols());
    int64_t size = 0;
    for (int64_t i = 0; i < spark_row_info.getNumRows(); i++)
    {
        reader.pointTo(
            reinterpret_cast<int64_t>(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i]), spark_row_info.getLengths()[i]);
        size += (reader.getStringSize(ordinal) + 1);
    }
    return size;
}

void writeRowToColumns(std::vector<MutableColumnPtr> & columns,std::vector<DataTypePtr>& types, SparkRowReader & spark_row_reader)
{
    int32_t num_fields = columns.size();
    bool is_nullable = false;
    for (int32_t i = 0; i < num_fields; i++)
    {
        WhichDataType which(columns[i]->getDataType());
        if (which.isNullable())
        {
            const auto * nullable = checkAndGetDataType<DataTypeNullable>(types[i].get());
            which = WhichDataType(nullable->getNestedType());
            is_nullable = true;
        }

        if (spark_row_reader.isNullAt(i)) {
            assert(is_nullable);
            ColumnNullable & column = assert_cast<ColumnNullable &>(*columns[i]);
            column.insertData(nullptr, 0);
            continue;
        }

        if (which.isUInt8())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(uint8_t));
        }
        else if (which.isInt8())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int8_t));
        }
        else if (which.isInt16())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int16_t));
        }
        else if (which.isInt32())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int32_t));
        }
        else if (which.isInt64())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(int64_t));
        }
        else if (which.isFloat32())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(float_t));
        }
        else if (which.isFloat64())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(double_t));
        }
        else if (which.isDate() || which.isUInt16())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(uint16_t));
        }
        else if (which.isDate32() || which.isUInt32())
        {
            columns[i]->insertData(spark_row_reader.getRawDataForFixedNumber(i), sizeof(uint32_t));
        }
        else if (which.isString())
        {
            StringRef data = spark_row_reader.getString(i);
            columns[i]->insertData(data.data, data.size);
        }
        else
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Doesn't support type {} convert from spark row to ch columnar",
                magic_enum::enum_name(columns[i]->getDataType()));
    }
}

std::unique_ptr<Block>
SparkRowToCHColumn::convertSparkRowInfoToCHColumn(local_engine::SparkRowInfo & spark_row_info, DB::Block & header)
{
    auto columns_list = std::make_unique<ColumnsWithTypeAndName>();
    columns_list->reserve(header.columns());
    std::vector<MutableColumnPtr> mutable_columns;
    std::vector<DataTypePtr> types;
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        MutableColumnPtr read_column = header_column.type->createColumn();
        read_column->reserve(spark_row_info.getNumRows());
        mutable_columns.push_back(std::move(read_column));
        types.push_back(header_column.type);
    }
    SparkRowReader row_reader(header.columns());
    for (int64_t i = 0; i < spark_row_info.getNumRows(); i++)
    {
        row_reader.pointTo(
            reinterpret_cast<int64_t>(spark_row_info.getBufferAddress() + spark_row_info.getOffsets()[i]), spark_row_info.getLengths()[i]);
        writeRowToColumns(mutable_columns, types, row_reader);
    }
    auto block = std::make_unique<Block>(*std::move(columns_list));
    for (size_t column_i = 0, columns = mutable_columns.size(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        ColumnWithTypeAndName column(std::move(mutable_columns[column_i]), header_column.type, header_column.name);
        block->insert(column);
    }
    mutable_columns.clear();
    return block;
}

void SparkRowToCHColumn::appendSparkRowToCHColumn(SparkRowToCHColumnHelper & helper, int64_t address, int32_t size)
{
    SparkRowReader row_reader(helper.header->columns());
    row_reader.pointTo(address, size);
    writeRowToColumns(*helper.cols, *helper.typePtrs, row_reader);
}

Block * SparkRowToCHColumn::getWrittenBlock(SparkRowToCHColumnHelper & helper)
{
    auto * block = new Block();
    for (size_t column_i = 0, columns = helper.cols->size(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = helper.header->getByPosition(column_i);
        ColumnWithTypeAndName column(std::move(helper.cols->operator[](column_i)), header_column.type, header_column.name);
        block->insert(column);
    }
    return block;
}

VariableLengthDataReader::VariableLengthDataReader(const DataTypePtr & type_) : type(type_), which(removeNullable(type))
{
    if (!BackingDataLengthCalculator::isVariableLengthDataType(type))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", type->getName());
}

Field VariableLengthDataReader::read(char *buffer, size_t length)
{
    if (which.isStringOrFixedString() )
        return std::move(readString(buffer, length));
    
    if (which.isDecimal128())
        return std::move(readDecimal(buffer, length));
    
    if (which.isArray())
        return std::move(readArray(buffer, length));
    
    if (which.isMap())
        return std::move(readMap(buffer, length));
    
    if (which.isTuple())
        return std::move(readStruct(buffer, length));

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", type->getName());
}

Field VariableLengthDataReader::readDecimal(char * buffer, size_t length)
{
    assert(sizeof(Decimal128) == length);

    Decimal128 value;
    memcpy(&value, buffer, length);
    return DecimalField<Decimal128>(value);
}

Field VariableLengthDataReader::readString(char * buffer, size_t length)
{
    String str(buffer, length);
    return std::move(Field(std::move(str)));
}

Field VariableLengthDataReader::readArray(char * buffer, [[maybe_unused]] size_t length)
{
    /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing data
    /// Read numElements
    int64_t num_elems = 0;
    memcpy(&num_elems, buffer, 8);
    if (num_elems == 0)
        return Array();

    /// Skip null_bitmap
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_elems);

    /// Read values
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    const auto & nested_type = array_type->getNestedType();
    const auto elem_size = BackingDataLengthCalculator::getArrayElementSize(nested_type);
    const auto len_values = roundNumberOfBytesToNearestWord(elem_size * num_elems);
    Array array;
    array.reserve(num_elems);

    if (BackingDataLengthCalculator::isFixedLengthDataType(nested_type))
    {
        FixedLengthDataReader reader(nested_type);
        for (int64_t i = 0; i < num_elems; ++i)
        {
            if (isBitSet(buffer + 8, i))
            {
                array.emplace_back(std::move(Null{}));
            }
            else
            {
                const auto elem = reader.read(buffer + 8 + len_null_bitmap + i * elem_size);
                array.emplace_back(elem);
            }
        }
    }
    else if (BackingDataLengthCalculator::isVariableLengthDataType(nested_type))
    {
        VariableLengthDataReader reader(nested_type);
        for (int64_t i = 0; i < num_elems; ++i)
        {
            if (isBitSet(buffer + 8, i))
            {
                array.emplace_back(std::move(Null{}));
            }
            else
            {
                int64_t offset_and_size = 0;
                memcpy(&offset_and_size, buffer + 8 + len_null_bitmap + i * 8, 8);
                const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
                const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);

                const auto elem = reader.read(buffer + offset, size);
                array.emplace_back(elem);
            }
        }
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", nested_type->getName());
    
    return std::move(array);
}

Field VariableLengthDataReader::readMap(char * buffer, size_t length)
{
    /// 内存布局：Length of UnsafeArrayData of key(8B) |  UnsafeArrayData of key | UnsafeArrayData of value
    /// Read Length of UnsafeArrayData of key
    int64_t key_array_size = 0;
    memcpy(&key_array_size, buffer, 8);
    if (key_array_size == 0)
        return std::move(Map());

    /// Read UnsafeArrayData of keys
    const auto * map_type = typeid_cast<const DataTypeMap *>(type.get());
    const auto & key_type = map_type->getKeyType();
    const auto key_array_type = std::make_shared<DataTypeArray>(key_type);
    VariableLengthDataReader key_reader(key_array_type);
    auto key_field = key_reader.read(buffer + 8, key_array_size);
    auto & key_array = key_field.safeGet<Array>();

    /// Read UnsafeArrayData of values
    const auto & val_type = map_type->getValueType();
    const auto val_array_type = std::make_shared<DataTypeArray>(val_type);
    VariableLengthDataReader val_reader(val_array_type);
    auto val_field = val_reader.read(buffer + 8 + key_array_size, length - 8 - key_array_size);
    auto & val_array = val_field.safeGet<Array>();

    /// Construct map in CH way [(k1, v1), (k2, v2), ...]
    if (key_array.size() != val_array.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key size {} not equal to value size {} in map", key_array.size(), val_array.size());
    Map map(key_array.size());
    for (size_t i = 0; i < key_array.size(); ++i)
    {
        Tuple tuple(2);
        tuple[0] = std::move(key_array[i]);
        tuple[1] = std::move(val_array[i]);
        
        map[i] = std::move(tuple);
    }
    return std::move(map);
}


Field VariableLengthDataReader::readStruct(char * buffer, size_t  /*length*/)
{
    /// 内存布局：null_bitmap(字节数与字段数成正比) | values(num_fields * 8B) | backing data
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
    const auto & field_types = tuple_type->getElements();
    const auto num_fields = field_types.size();
    if (num_fields == 0)
        return std::move(Tuple());
    
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_fields);
    
    Tuple tuple(num_fields);
    for (size_t i=0; i<num_fields; ++i)
    {
        const auto & field_type = field_types[i];
        if (isBitSet(buffer, i))
        {
            tuple[i] = std::move(Null{});
            continue;
        }

        if (BackingDataLengthCalculator::isFixedLengthDataType(field_type))
        {
            FixedLengthDataReader reader(field_type);
            tuple[i] = std::move(reader.read(buffer + len_null_bitmap + i * 8));
        }
        else if (BackingDataLengthCalculator::isVariableLengthDataType(field_type))
        {
            int64_t offset_and_size = 0;
            memcpy(&offset_and_size, buffer + len_null_bitmap + i * 8, 8);
            const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
            const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);

            VariableLengthDataReader reader(field_type);
            tuple[i] = std::move(reader.read(buffer + offset, size));
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", field_type->getName());
    }
    return std::move(tuple);
}

FixedLengthDataReader::FixedLengthDataReader(const DataTypePtr & type_) : type(type_), which(type)
{
}

Field FixedLengthDataReader::read(char * buffer)
{
    if (which.isUInt8())
    {
        UInt8 value = 0;
        memcpy(&value, buffer, 1);
        return value;
    }

    if (which.isUInt16() || which.isDate())
    {
        UInt16 value = 0;
        memcpy(&value, buffer, 2);
        return value;
    }

    if (which.isUInt32() || which.isDate32())
    {
        UInt32 value = 0;
        memcpy(&value, buffer, 4);
        return value;
    }

    if (which.isUInt64())
    {
        UInt64 value = 0;
        memcpy(&value, buffer, 8);
        return value;
    }

    if (which.isInt8())
    {
        Int8 value = 0;
        memcpy(&value, buffer, 1);
        return value;
    }

    if (which.isInt16())
    {
        Int16 value = 0;
        memcpy(&value, buffer, 2);
        return value;
    }

    if (which.isInt32())
    {
        Int32 value = 0;
        memcpy(&value, buffer, 4);
        return value;
    }

    if (which.isInt64())
    {
        Int64 value = 0;
        memcpy(&value, buffer, 8);
        return value;
    }

    if (which.isFloat32())
    {
        Float32 value = 0.0;
        memcpy(&value, buffer, 4);
        return value;
    }

    if (which.isFloat64())
    {
        Float64 value = 0.0;
        memcpy(&value, buffer, 8);
        return value;
    }

    if (which.isDecimal32())
    {
        Decimal32 value = 0;
        memcpy(&value, buffer, 4);
        return value;
    }

    if (which.isDecimal64() || which.isDateTime64())
    {
        Decimal64 value = 0;
        memcpy(&value, buffer, 8);
        return value;
    }
    throw Exception(ErrorCodes::UNKNOWN_TYPE, "FixedLengthDataReader doesn't support type {}", type->getName());
}

}
