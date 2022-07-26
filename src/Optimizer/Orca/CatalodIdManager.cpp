#include <Optimizer/Orca/CatalogIdManager.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
UInt64 CatalogIdManager::nextRelationId()
{
    auto id = current_rel_id++;
    if (id > ID_REL_STATS_END)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Relation catalog id is overflow. current is {}, upper limit is {}", id, ID_REL_STATS_END);
    }
    return id;
}
}
