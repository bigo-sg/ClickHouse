#include "SparkRowToCHColumn.h"
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
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
        return readString(buffer, length);
    
    if (which.isDecimal128())
        return readDecimal(buffer, length);
    
    if (which.isArray())
        return readArray(buffer, length);
    
    if (which.isMap())
        return readMap(buffer, length);
    
    if (which.isTuple())
        return readTuple(buffer, length);

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
    return {std::move(String(buffer, length))};
}

Field VariableLengthDataReader::readArray(char * buffer, [[maybe_unused]] size_t length)
{
    /// 内存布局：numElements(8B) | null_bitmap(与numElements成正比) | values(每个值长度与类型有关) | backing data
    char * pos = buffer;

    /// Read numElements
    int64_t num_elems = 0;
    memcpy(&num_elems, pos, 8);
    if (num_elems == 0)
        return Array();
    pos += 8;

    /// Skip null_bitmap
    const auto len_null_bitmap = calculateBitSetWidthInBytes(num_elems);
    pos += len_null_bitmap;

    /// Read values
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    const auto & nested_type = array_type->getNestedType();
    const auto elem_size = BackingDataLengthCalculator::getArrayElementSize(nested_type);
    const auto len_values = roundNumberOfBytesToNearestWord(elem_size * num_elems);

    Array array;
    array.reserve(num_elems);

    std::unique_ptr<FixedLengthDataReader> reader;
    if (BackingDataLengthCalculator::isFixedLengthDataType(nested_type))
        reader = std::make_unique<FixedLengthDataReader>(nested_type);
    else if (BackingDataLengthCalculator::isVariableLengthDataType(nested_type))
        reader = std::make_unique<FixedLengthDataReader>(std::make_shared<DataTypeInt64>());
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "VariableLengthDataReader doesn't support type {}", nested_type->getName());

    for (int64_t i=0; i<num_elems; ++i)
    {
        auto field = reader->read(buffer + 8 + len_null_bitmap + elem_size * i);
        array.emplace_back(std::move(field));
    }
    pos += len_values;

    /// Read backing data
    for (int64_t i=0; i<num_elems; ++i)
    {
        const auto & offset_and_size = array[i].get<Int64>();
        const auto offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
        const auto size = BackingDataLengthCalculator::extractOffset(offset_and_size);
    }
}

Field VariableLengthDataReader::readMap(char * buffer, size_t length)
{

}

Field VariableLengthDataReader::readTuple(char * buffer, size_t length)
{

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
