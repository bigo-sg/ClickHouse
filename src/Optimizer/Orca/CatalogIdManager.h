#pragma once

#include "base/types.h"
namespace DB
{
class CatalogIdManager
{
public:
    enum CatalogIdRange
    {
        ID_DB_START = 1,
        ID_DB_END = 100000,
        ID_COL_STATS_START = 100001,
        ID_COL_STATS_END = 200000,
        ID_REL_STATS_START = 200001,
        ID_REL_STATS_END = 300000,
        ID_CAST_FUNC_START = 300001,
        ID_CAST_FUNC_END = 400000,
        ID_SC_CMP_START = 400001,
        ID_SC_CMP_END = 500000,
        ID_DB_CTAS_START = 500001,
        ID_DB_CATS_END = 600000,
    };

    CatalogIdManager() : current_rel_id(ID_REL_STATS_START) {}

    UInt64 nextRelationId();
private:
    UInt64 current_rel_id;
};
}
