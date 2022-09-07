#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/BatchParquetFileSource.h>
#include <Parser/SerializedPlanParser.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

int main()
{
    using namespace DB;
    using namespace local_engine;

    Block header{
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_orderkey"),
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_partkey"),
        ColumnWithTypeAndName(DataTypeInt64().createColumn(), std::make_shared<DataTypeInt64>(), "l_suppkey")};
    std::string file = "file:///data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";

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
        std::cout << "add rows:" << res.rows() << std::endl; 
        rows += res.rows();
    }
    std::cout << "rows:" << rows << std::endl;
    return 0;
}