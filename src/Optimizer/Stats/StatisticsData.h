#pragma once
#include <memory>
#include <Core/Types.h>
#include <base/types.h>
#include <Interpreters/StorageID.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Storages/IStorage_fwd.h>
namespace DB
{
class ColumnStatistics
{
public:
    Strings name_parts; // maybe a nested column
    DataTypePtr type;

    bool is_stats_missing = false;
    Float32 null_freq = 0.0; // count(null value) / rows
    Float32 ndv_freq = 0.0; // distinct(value) / rows
};
using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;
using ColumnStatisticsList = std::vector<ColumnStatisticsPtr>;

class TableStatistics
{
public:
    const ColumnStatisticsList & getPhysicalColumns() const;
    const ColumnStatisticsList & getHiddenColumns() const;

    /// database and table name
    StoragePtr getStorage() const;
    UInt64 getRows() const { return rows; }
private:
    friend class StatisticsAccessor;

    ColumnStatisticsList physical_columns;
    ColumnStatisticsList hidden_columns;
    UInt64 rows = 0;

    /// To accesss parttition kyes, sorted columns
    StoragePtr storage; 
};
using TableStatisticsPtr = std::shared_ptr<TableStatistics>;
using TableStatisticsList = std::vector<TableStatisticsPtr>;


}
