#include <gtest/gtest.h>
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
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/ch_parquet/OptimizedParquetBlockInputFormat.h>
#include <Storages/ch_parquet/OptimizedArrowColumnToCHColumn.h>
#include <Storages/ch_parquet/arrow/reader.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/DebugUtils.h>

using namespace DB;

TEST(ParquetRead, ReadSchema)
{
    const String path = "/data1/liyang/cppproject/kyli/ClickHouse/utils/local-engine/tests/data/alltypes/part-00000-f352a2d1-97fd-4244-85bc-eb6fcd8c9da3-c000.snappy.parquet";
    auto in = std::make_shared<ReadBufferFromFile>(path);
    FormatSettings settings;
    OptimizedParquetSchemaReader schema_reader(*in, settings);
    auto name_and_types = schema_reader.readSchema();
    auto & factory = DataTypeFactory::instance();

    auto check_type = [&name_and_types, &factory](const String & column, const String & expect_str_type)
    {
        auto expect_type = factory.get(expect_str_type);

        auto name_and_type = name_and_types.tryGetByName(column);
        EXPECT_TRUE(name_and_type);

        std::cout << "real_type:" << name_and_type->type->getName() << ", expect_type:" << expect_type->getName() << std::endl;
        EXPECT_TRUE(name_and_type->type->equals(*expect_type));
    };

    check_type("f_bool", "Nullable(UInt8)");
    check_type("f_byte", "Nullable(Int8)");
    check_type("f_short", "Nullable(Int16)");
    check_type("f_int", "Nullable(Int32)");
    check_type("f_long", "Nullable(Int64)");
    check_type("f_float", "Nullable(Float32)");
    check_type("f_double", "Nullable(Float64)");
    check_type("f_string", "Nullable(String)");
    check_type("f_binary", "Nullable(String)");
    check_type("f_decimal", "Nullable(Decimal(10, 2))");
    check_type("f_date", "Nullable(Date32)");
    check_type("f_timestamp", "Nullable(DateTime64(9))");
    check_type("f_array", "Nullable(Array(Nullable(String)))");
    check_type("f_array_array", "Nullable(Array(Nullable(Array(Nullable(String)))))");
    check_type("f_array_map", "Nullable(Array(Nullable(Map(String, Nullable(Int64)))))");
    check_type("f_array_struct", "Nullable(Array(Nullable(Tuple(a Nullable(String), b Nullable(Int64)))))");
    check_type("f_map", "Nullable(Map(String, Nullable(Int64)))");
    check_type("f_map_map", "Nullable(Map(String, Nullable(Map(String, Nullable(Int64)))))");
    check_type("f_map_array", "Nullable(Map(String, Nullable(Array(Nullable(Int64)))))");
    check_type("f_map_struct", "Nullable(Map(String, Nullable(Tuple(a Nullable(String), b Nullable(Int64)))))");
    check_type("f_struct", "Nullable(Tuple(a Nullable(String), b Nullable(Int64)))");
    check_type("f_struct_struct", "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Tuple(x Nullable(String), y Nullable(Int64)))))");
    check_type("f_struct_array", "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Array(Nullable(Int64)))))");
    check_type("f_struct_map", "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Map(String, Nullable(Int64)))))");
}

TEST(ParquetRead, ReadData)
{
    const String path = "/data1/liyang/cppproject/kyli/ClickHouse/utils/local-engine/tests/data/alltypes/part-00000-f352a2d1-97fd-4244-85bc-eb6fcd8c9da3-c000.snappy.parquet";
    auto in = std::make_shared<ReadBufferFromFile>(path);
    FormatSettings settings;
    OptimizedParquetSchemaReader schema_reader(*in, settings);
    auto name_and_types = schema_reader.readSchema();

    ColumnsWithTypeAndName columns;
    columns.reserve(name_and_types.size());
    std::set<String> names{
        /*
        "f_bool",
        "f_byte",
        "f_short",
        "f_int",
        "f_long",
        "f_float",
        "f_double",
        "f_string",
        "f_binary",
        "f_decimal",
        "f_date",
        "f_timestamp",
        */
        "f_array",
    };

    for (const auto & name_and_type : name_and_types)
    {
        if (names.count(name_and_type.name))
            columns.emplace_back(name_and_type.type, name_and_type.name);
    }
    Block header(columns);
    in = std::make_shared<ReadBufferFromFile>(path);
    auto format = std::make_shared<OptimizedParquetBlockInputFormat>(*in, header, settings);
    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);

    Block res;
    while(reader->pull(res))
    {
        debug::headBlock(res);
    }
}

