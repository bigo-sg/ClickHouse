#include <memory>
#include <Optimizer/Stats/StatisticsAccessor.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Stats/StatisticsData.h>
#include <Common/logger_useful.h>
#include <Interpreters/StorageID.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{
StatisticsAccessor::StatisticsAccessor() = default;

StatisticsAccessor & StatisticsAccessor::instance()
{
    static StatisticsAccessor accessor;
    return accessor;
}

#if 1


/// We have a table "create table default.test1 (a Int32, b Int32, day String) partition by day  
TableStatisticsList StatisticsAccessor::genFakeData(ContextPtr context)
{
    ColumnStatisticsPtr a_col = std::make_shared<ColumnStatistics>();
    a_col->name_parts = {String("a")};
    a_col->type = std::make_shared<DataTypeInt32>();
    a_col->null_freq = 0.0;
    a_col->ndv_freq = 0.01;

    ColumnStatisticsPtr b_col = std::make_shared<ColumnStatistics>();
    b_col->name_parts = {String("b")};
    b_col->type = std::make_shared<DataTypeInt32>();
    b_col->null_freq = 0.0;
    b_col->ndv_freq = 0.01;
    
    ColumnStatisticsPtr day_col = std::make_shared<ColumnStatistics>();
    day_col->name_parts = {String("day")};
    day_col->type = std::make_shared<DataTypeString>();
    day_col->null_freq = 0.0;
    day_col->ndv_freq = 0.01;

    TableStatisticsPtr table_stats = std::make_shared<TableStatistics>();
    table_stats->physical_columns = {a_col, b_col, day_col};
    table_stats->rows = 100;

    StorageID storage_id("default", "test1");
    table_stats->storage = DatabaseCatalog::instance().getTable(storage_id, context);

    return {table_stats};
}

TableStatisticsList StatisticsAccessor::getStatistics(ContextPtr context, ASTPtr /*query*/)
{
    LOG_DEBUG(logger, "get fake statistics");
    return genFakeData(context);
}
#else
#endif
}
