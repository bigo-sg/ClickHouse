#include <mutex>
#include <Storages/ShuffleJoin/DistributedKeyHashStorage.h>
#include <Common/ErrorCodes.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

HashedChunkTablePtr KeyHashStroageSessionTables::getTable(const Block & header_, const String & table_id)
{
    std::lock_guard lock(mutex);
    auto iter = tables.find(table_id);
    if (iter != tables.end())
    {
        if (!blocksHaveEqualStructure(header_, iter->second->header))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unequal header. {} vs. {}", header_.dumpNames(), iter->second->header.dumpNames());
        return iter->second;
    }
    auto table = std::make_shared<HashedChunkTable>(header_);
    tables[table_id] = table;
    return table;
}

HashedChunkTablePtr DistributedKeyHashStorage::getTable(const String & session_id_, const String & table_id_)
{
    std::lock_guard lock(mutex);
    auto session_iter = sessions.find(session_id_);
    if (session_iter == sessions.end())
        return nullptr;
    auto & session = session_iter->second;
    return session->getTable(table_id_);
}

HashedChunkTablePtr DistributedKeyHashStorage::getTable(const Block & header_, const String & session_id_, const String & table_id_)
{
    std::lock_guard lock(mutex);
    auto session_iter = sessions.find(session_id_);
    if (session_iter == sessions.end())
        return nullptr;
    auto & session = session_iter->second;
    return session->getTable(header_, table_id_);
}


}
