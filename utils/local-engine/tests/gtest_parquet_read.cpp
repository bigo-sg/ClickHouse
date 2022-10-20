#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
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
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
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
    ParquetSchemaReader schema_reader(*in, settings);
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
    ParquetSchemaReader schema_reader(*in, settings);
    auto name_and_types = schema_reader.readSchema();

    ColumnsWithTypeAndName columns;
    columns.reserve(name_and_types.size());
    std::map<String, Field> fields{
        {"f_bool", UInt8(1)},
        {"f_byte", Int8(1)},
        {"f_short", Int16(2)},
        {"f_int", Int32(3)},
        {"f_long", Int64(4)},
        {"f_float", Float32(5.5)},
        {"f_double", Float64(6.6)},
        {"f_string", "hello world"},
        {"f_binary", "hello world"},
        {"f_decimal", DecimalField<Decimal64>(777, 2)},
        {"f_date", Int32(18262)},
        {"f_timestamp", DecimalField<DateTime64>(1666162060000000L, 6)},
        {"f_array", Array{"hello", "world"}},
        {"f_array_array", []() -> Field
            {
                Array res;
                res.push_back(Array{"hello"});
                res.push_back(Array{"world"});
                return std::move(res);
            }(),
        },
        {"f_array_map", []() -> Field
            {
                Array res;

                Map map;
                map.push_back(Tuple{"hello", Int64(1)});
                res.push_back(map);
                
                map.clear();
                map.push_back(Tuple{"world", Int64(2)});
                res.push_back(map);

                return std::move(res);
            }(),
        },
        {"f_array_struct", []() -> Field
            {
                Array res;
                res.push_back(Tuple{"hello", Int64(1)});
                res.push_back(Tuple{"world", Int64(2)});

                return std::move(res);
                
            }(),
        },
        {"f_map", []() -> Field
            {
                Map res;
                res.push_back(Tuple{"hello", Int64(1)});
                res.push_back(Tuple{"world", Int64(2)});
                return std::move(res);
            }(),
        },
        {"f_map_map", []() -> Field
            {
                Map nested_map;
                nested_map.push_back(Tuple{"world", Int64(3)});

                Map res;
                res.push_back(Tuple{"hello", std::move(nested_map)});
                return std::move(res);
            }(),
        },
        {"f_map_array", []() -> Field
            {
                Array array{Int64(1),Int64(2),Int64(3)};
                
                Map res;
                res.push_back(Tuple{"hello", std::move(array)});
                return std::move(res);
            }(),
        },
        {"f_map_struct", []() -> Field
            {
                Tuple tuple{"world", Int64(4)};

                Map res;
                res.push_back(Tuple{"hello", std::move(tuple)});
                return std::move(res);
            }(),
        },
        {"f_struct", []() -> Field
            {
                Tuple res{"hello world", Int64(5)};
                return std::move(res);
            }(),
        },
        {"f_struct_struct", []() -> Field
            {
                Tuple tuple{"world", Int64(6)};
                Tuple res{"hello", Int64(6), std::move(tuple)};
                return std::move(res);
            }(),
        },
        {"f_struct_array", []() -> Field
            {
                Array array{Int64(1), Int64(2), Int64(3)};
                Tuple res{"hello", Int64(7), std::move(array)};
                return std::move(res);
            }(),
        },
        {"f_struct_map", []() -> Field
            {
                Map map;
                map.push_back(Tuple{"world", Int64(9)});

                Tuple res{"hello", Int64(8), std::move(map)};
                return std::move(res);
            }(),
        },
    };

    for (const auto & name_and_type : name_and_types)
        if (fields.count(name_and_type.name))
            columns.emplace_back(name_and_type.type, name_and_type.name);

    Block header(columns);
    in = std::make_shared<ReadBufferFromFile>(path);
    auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, settings);
    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);

    Block block;
    EXPECT_TRUE(reader->pull(block));
    EXPECT_TRUE(block.rows() == 1);

    for (const auto & name_and_type : name_and_types)
    {
        if (fields.count(name_and_type.name))
        {
            const auto & name = name_and_type.name;
            const auto & column = block.getByName(name);
            auto field = (*column.column)[0];
            auto expect_field = fields[name];
            std::cout << "field:" << toString(field) << ", expect_field:" << toString(expect_field) << std::endl;
            EXPECT_TRUE(field == expect_field);
        }
    }

}


