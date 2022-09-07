#include <benchmark/benchmark.h>


#include <parquet/arrow/reader.h>

#include <Core/Block.h>
#include <Common/DebugUtils.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ch_parquet/OptimizedParquetBlockInputFormat.h>
#include <Storages/ch_parquet/OptimizedArrowColumnToCHColumn.h>
#include <Storages/ch_parquet/arrow/reader.h>
#include <Storages/BatchParquetFileSource.h>
#include <Parser/SerializedPlanParser.h>

static void helper(const DB::Block & header, const std::string & file, const DB::FormatSettings & format_settings = {})
{
    using namespace DB;

    auto in = std::make_unique<ReadBufferFromFile>(file);

    auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, format_settings);

    auto pipeline = QueryPipeline(std::move(format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);

    Block res;
    int rows = 0;
    while (reader->pull(res))
    {
        rows += res.rows();
    }
    std::cerr << "rows:" << rows << std::endl;;
}

static void optimizedHelper(const DB::Block & header, const std::string & file)
{
    using namespace DB;
    using namespace local_engine;

    auto files_info = std::make_shared<local_engine::FilesInfo>();
    files_info->files = {file};

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe(
        std::make_shared<local_engine::BatchParquetFileSource>(files_info, header, local_engine::SerializedPlanParser::global_context)));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto reader = PullingPipelineExecutor(pipeline);

    Block res;
    int rows = 0;
    while (reader.pull(res))
    {
        rows += res.rows();
    }
    std::cerr << "optimized rows:" << rows << std::endl;;
}

static void BM_ParquetReadString(benchmark::State& state)
{
    using namespace DB;
    Block header{
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_returnflag"),
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_linestatus")
    };
    std::string file
        = "/data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    FormatSettings format_settings;

    for (auto _ : state)
    {
        helper(header, file, format_settings);
    }
}


static void BM_ParquetReadDate32(benchmark::State& state)
{
    using namespace DB;
    Block header{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")
    };
    std::string file
        = "/data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    FormatSettings format_settings;
    for (auto _ : state)
    {
        helper(header, file, format_settings);
    }
}


static void BM_OptimizedParquetReadString(benchmark::State& state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_returnflag"),
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_linestatus")
    };
    std::string file = "file:///data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    Block res;

    for (auto _ : state)
    {
        optimizedHelper(header, file);
    }
}



static void BM_OptimizedParquetReadDate32(benchmark::State& state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")
    };
    std::string file = "file:///data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    for (auto _ : state)
    {
        optimizedHelper(header, file);
    }
}


static void BM_ParquetReadInt64(benchmark::State& state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_orderkey"),
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_partkey"),
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_suppkey")
    };
    std::string file = "/data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    for (auto _ : state)
    {
        helper(header, file);
    }
}


static void BM_OptimizedParquetReadInt64(benchmark::State& state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_orderkey"),
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_partkey"),
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_suppkey")
    };
    std::string file = "file:///data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    for (auto _ : state)
    {
        optimizedHelper(header, file);
    }
}


BENCHMARK(BM_ParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);

BENCHMARK(BM_ParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(10);

BENCHMARK(BM_ParquetReadInt64)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadInt64)->Unit(benchmark::kMillisecond)->Iterations(10);

