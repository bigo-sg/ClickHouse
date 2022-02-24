#pragma once
#include <base/types.h>
#include <Processors/Chunk.h>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <Core/Block.h>

namespace DB
{
class HashedChunkTable
{
public:
    explicit HashedChunkTable(const Block & header_) : header(header_) {}

    Block header;
    std::list<Chunk> chunks;
};
using HashedChunkTablePtr = std::shared_ptr<HashedChunkTable>;


class KeyHashStroageSessionTables
{
public:
    inline HashedChunkTablePtr getTable(const String & table_id)
    {
        std::lock_guard lock(mutex);
        auto iter = tables.find(table_id);
        if (iter == tables.end())
            return nullptr;
        return iter->second;
    }

    HashedChunkTablePtr getTable(const Block & header_, const String & table_id);

private:
    std::unordered_map<String, HashedChunkTablePtr> tables;
    std::mutex mutex;
};
using KeyHashStroageSessionTablesPtr = std::shared_ptr<KeyHashStroageSessionTables>;

// Only one global instance
class DistributedKeyHashStorage
{
public:
    HashedChunkTablePtr getTable(const String & session_id_, const String & table_id_);
    HashedChunkTablePtr getTable(const Block & header_, const String & session_id, const String & table_id);
    void dropTables(const String & session_id_);
    void dropTable(const String & session_id_, const String & table_id_);

private:
    std::unordered_map<String, KeyHashStroageSessionTablesPtr> sessions;
    std::mutex mutex;
};
}
