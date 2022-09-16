#include "PartitionColumnFillingTransform.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/StringUtils.h>
#include "Processors/Chunk.h"
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <base/DayNum.h>

#include <Poco/Logger.h>
#include <base/logger_useful.h>


using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
template <typename Type>
    requires(std::is_same_v<Type, Int8> || std::is_same_v<Type, UInt16> || std::is_same_v<Type, Int16> || std::is_same_v<Type, Int32> || std::is_same_v<Type, Int64>)
ColumnPtr createIntPartitionColumn(DataTypePtr column_type, std::string partition_value, size_t rows)
{
    Type value;
    auto value_buffer = ReadBufferFromString(partition_value);
    readIntText(value, value_buffer);
    return column_type->createColumnConst(rows, value);
}

template <typename Type>
    requires(std::is_same_v<Type, Float32> || std::is_same_v<Type, Float64>)
ColumnPtr createFloatPartitionColumn(DataTypePtr column_type, std::string partition_value, size_t rows)
{
    Type value;
    auto value_buffer = ReadBufferFromString(partition_value);
    readFloatText(value, value_buffer);
    return column_type->createColumnConst(rows, value);
}


PartitionColumnFillingTransform::PartitionColumnFillingTransform(
    const DB::Block & input_, const DB::Block & output_, const PartitionValues & partition_columns_)
    : ISimpleTransform(input_, output_, true), partition_column_values(partition_columns_)
{
    for (const auto & value : partition_column_values)
    {
        partition_columns[value.first] = value.second;
    }
}

/// In the case that a partition column is wrapper by nullable or LowCardinality, we need to keep the data type same
/// as input.
ColumnPtr PartitionColumnFillingTransform::tryWrapPartitionColumn(const ColumnPtr & nested_col, DataTypePtr original_data_type)
{
    auto result = nested_col;
    if (original_data_type->getTypeId() == TypeIndex::Nullable)
    {
        result = ColumnNullable::create(nested_col, ColumnUInt8::create());
    }
    return result;
}

ColumnPtr PartitionColumnFillingTransform::createPartitionColumn(const String & parition_col, const String & partition_col_value, size_t rows)
{
    ColumnPtr result;
    auto partition_col_type = output.getHeader().getByName(parition_col).type;
    DataTypePtr nested_type = partition_col_type;
    if (const DataTypeNullable * nullable_type = checkAndGetDataType<DataTypeNullable>(partition_col_type.get()))
    {
        nested_type = nullable_type->getNestedType();
        if (StringUtils::isNullPartitionValue(partition_col_value))
        {
            return nullable_type->createColumnConstWithDefaultValue(1);
        }
    }
    WhichDataType which(nested_type);
    if (which.isInt8())
    {
        result = createIntPartitionColumn<Int8>(nested_type, partition_col_value, rows);
    }
    else if (which.isInt16())
    {
        result = createIntPartitionColumn<Int16>(nested_type, partition_col_value, rows);
    }
    else if (which.isInt32())
    {
        result = createIntPartitionColumn<Int32>(nested_type, partition_col_value, rows);
    }
    else if (which.isInt64())
    {
        result = createIntPartitionColumn<Int64>(nested_type, partition_col_value, rows);
    }
    else if (which.isFloat32())
    {
        result = createFloatPartitionColumn<Float32>(nested_type, partition_col_value, rows);
    }
    else if (which.isFloat64())
    {
        result = createFloatPartitionColumn<Float64>(nested_type, partition_col_value, rows);
    }
    else if (which.isDate())
    {
        DayNum value;
        auto value_buffer = ReadBufferFromString(partition_col_value);
        readDateText(value, value_buffer);
        result = nested_type->createColumnConst(rows, value);
    }
    else if (which.isDate32())
    {
        ExtendedDayNum value;
        auto value_buffer = ReadBufferFromString(partition_col_value);
        readDateText(value, value_buffer);
        result = nested_type->createColumnConst(rows, value.toUnderType());
    }
    else if (which.isString())
    {
        result = nested_type->createColumnConst(rows, partition_col_value);
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported datatype {}", partition_col_type->getFamilyName());
    }
    result = tryWrapPartitionColumn(result, partition_col_type);
    return result;
}

void PartitionColumnFillingTransform::transform(DB::Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    auto input_cols = chunk.detachColumns();
    Columns result_cols;
    auto input_header = input.getHeader();
    for (const auto & output_col : output.getHeader())
    {
        if (input_header.has(output_col.name))
        {
            size_t pos = input_header.getPositionByName(output_col.name);
            result_cols.push_back(input_cols[pos]);
        }
        else
        {
            // it's a partition column
            auto it = partition_columns.find(output_col.name);
            if (it == partition_columns.end())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found column({}) in parition columns", output_col.name);
            }
            result_cols.emplace_back(createPartitionColumn(it->first, it->second, rows));

        }
        
    }
    chunk = DB::Chunk(std::move(result_cols), rows);
}
}
