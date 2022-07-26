#include <Optimizer/Stats/StatisticsData.h>
#include "Storages/IStorage_fwd.h"

namespace DB
{
const ColumnStatisticsList & TableStatistics::getPhysicalColumns() const
{
    return physical_columns;
}

const ColumnStatisticsList & TableStatistics::getHiddenColumns() const
{
    return hidden_columns;
}

StoragePtr TableStatistics::getStorage() const
{
    return storage;
}
}
