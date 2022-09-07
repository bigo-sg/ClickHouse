#include <gtest/gtest.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/ch_parquet/arrow/encoding.h>
#include <Storages/BatchParquetFileSource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <parquet/schema.h>

using namespace local_engine;

TEST(TestStringUtils, TestExtractPartitionValues)
{
    std::string path = "/tmp/col1=1/col2=test/a.parquet";
    auto values = StringUtils::parsePartitionTablePath(path);
    ASSERT_EQ(2, values.size());
    ASSERT_EQ("col1", values[0].first);
    ASSERT_EQ("1", values[0].second);
    ASSERT_EQ("col2", values[1].first);
    ASSERT_EQ("test", values[1].second);
}


TEST(TestDecoder, TestDecoder)
{
    using namespace ch_parquet;
    using namespace DB;
    auto node = parquet::schema::PrimitiveNode::Make("a", Repetition::REQUIRED, Type::INT64);
    auto descr = std::make_shared<ColumnDescriptor>(node, 1, 1, nullptr);
    std::unique_ptr<TypedDecoder<Int64Type>> decoder = MakeTypedDecoder<Int64Type>(Encoding::PLAIN);
    
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    auto column = type->createColumn();
    decoder->DecodeClickHouse(0, 0, nullptr, 0, *column);
}

   


