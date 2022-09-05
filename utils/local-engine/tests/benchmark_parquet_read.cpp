#include <benchmark/benchmark.h>


// #include <parquet/arrow/reader.h>

#include <Core/Block.h>
#include <Common/DebugUtils.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
// #include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
// #include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ch_parquet/OptimizedParquetBlockInputFormat.h>
#include <Storages/ch_parquet/OptimizedArrowColumnToCHColumn.h>
#include <Storages/ch_parquet/arrow/reader.h>


/*
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
    Block res;

    for (auto _ : state)
    {
        auto in = std::make_unique<ReadBufferFromFile>(file);
        auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, format_settings);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            debug::headBlock(res);
        }
    }
}
*/


static void BM_OptimizedParquetReadString(benchmark::State& state)
{
    using namespace DB;
    Block header{
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_returnflag"),
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_linestatus")
    };
    std::string file
        = "/data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    FormatSettings format_settings;
    Block res;

    for (auto _ : state)
    {
        auto in = std::make_unique<ReadBufferFromFile>(file);
        auto format = std::make_shared<OptimizedParquetBlockInputFormat>(*in, header, format_settings);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            debug::headBlock(res);
        }
    }
}


// BENCHMARK(BM_ParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);

