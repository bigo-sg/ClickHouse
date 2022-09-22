#include <gtest/gtest.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SparkRowToCHColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDate.h>

using namespace local_engine;
using namespace DB;


TEST(CHColumnToSparkRow, BitSetWidthCalculation)
{
    EXPECT_TRUE(calculateBitSetWidthInBytes(0) == 0);
    EXPECT_TRUE(calculateBitSetWidthInBytes(1) == 8);
    EXPECT_TRUE(calculateBitSetWidthInBytes(32) == 8);
    EXPECT_TRUE(calculateBitSetWidthInBytes(64) == 8);
    EXPECT_TRUE(calculateBitSetWidthInBytes(65) == 16);
    EXPECT_TRUE(calculateBitSetWidthInBytes(128) == 16);
}

TEST(CHColumnToSparkRow, GetArrayElementSize)
{
    const std::map<DataTypePtr, int64_t> type_to_size = {
        {std::make_shared<DataTypeInt8>(), 1},
        {std::make_shared<DataTypeUInt8>(), 1},
        {std::make_shared<DataTypeInt16>(), 2},
        {std::make_shared<DataTypeUInt16>(), 2},
        {std::make_shared<DataTypeDate>(), 2},
        {std::make_shared<DataTypeInt32>(), 4},
        {std::make_shared<DataTypeUInt32>(), 4},
        {std::make_shared<DataTypeFloat32>(), 4},
        {std::make_shared<DataTypeDate32>(), 4},
        {std::make_shared<DataTypeDecimal32>(9, 4), 4},
        {std::make_shared<DataTypeInt64>(), 8},
        {std::make_shared<DataTypeUInt64>(), 8},
        {std::make_shared<DataTypeFloat64>(), 8},
        {std::make_shared<DataTypeDateTime64>(6), 8},
        {std::make_shared<DataTypeDecimal64>(18, 4), 8},

        {std::make_shared<DataTypeString>(), 8},
        {std::make_shared<DataTypeDecimal128>(38, 4), 8},
        {std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeString>()), 8},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()), 8},
        {std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeString>()}), 8},
    };

    for (const auto & [type, size] : type_to_size)
    {
        EXPECT_TRUE(BackingDataLengthCalculator::getArrayElementSize(type) == size);
        if (type->canBeInsideNullable())
        {
            const auto type_with_nullable = std::make_shared<DataTypeNullable>(type);
            EXPECT_TRUE(BackingDataLengthCalculator::getArrayElementSize(type_with_nullable) == size);
        }
    }
}

TEST(CHColumnToSparkRow, PrimitiveTypes)
{
    ColumnsWithTypeAndName columns = {
        {nullptr, std::make_shared<DataTypeInt64>(), "f_int64"},
        {nullptr, std::make_shared<DataTypeUInt64>(), "f_uint64"},
        {nullptr, std::make_shared<DataTypeInt32>(), "f_int32"},
        {nullptr, std::make_shared<DataTypeUInt32>(), "f_uint32"},
    };

    Block block(columns);
    auto mutable_colums = block.mutateColumns();
    mutable_colums[0]->insert(-1);
    mutable_colums[1]->insert(1);
    mutable_colums[2]->insert(-2);
    mutable_colums[3]->insert(2);
    block.setColumns(std::move(mutable_colums));

    auto converter = CHColumnToSparkRow();
    auto spark_row_info = converter.convertCHColumnToSparkRow(block);
    EXPECT_TRUE(spark_row_info->getTotalBytes() == 8 + 4 * 8);

    auto reader = SparkRowReader(spark_row_info->getNumCols(), block.getDataTypes());
    reader.pointTo(spark_row_info->getBufferAddress(), spark_row_info->getTotalBytes());
    EXPECT_TRUE(reader.get(0).get<Int64>() == -1);
    EXPECT_TRUE(reader.get(1).get<UInt64>() == 1);
    EXPECT_TRUE(reader.get(2).get<Int32>() == -2);
    EXPECT_TRUE(reader.get(3).get<Int32>() == 2);
}
