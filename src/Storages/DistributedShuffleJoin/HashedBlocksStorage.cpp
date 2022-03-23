#include <memory>
#include <mutex>
#include <Storages/DistributedShuffleJoin/HashedBlocksStorage.h>
#include <base/logger_useful.h>
#include <Common/ErrorCodes.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

Chunk TableHashedBlocksStorage::popChunkWithoutMutex()
{
    if (unlikely(chunks.empty()))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "table({}.{}) has empty chunks", session_id, table_id);
    }
    Chunk res;
    res.swap(chunks.front());
    chunks.pop_front();
    return res;
}

TableHashedBlocksStoragePtr SessionHashedBlocksTablesStorage::getTable(const String & table_id_) const
{
    std::lock_guard lock(mutex);
    auto iter = tables.find(table_id_);
    if (iter == tables.end())
    {
        LOG_INFO(logger, "Table({}) not found in session({})", table_id_, session_id);
        return nullptr;
    }
    return iter->second;
}

TableHashedBlocksStoragePtr SessionHashedBlocksTablesStorage::getOrSetTable(const String & table_id_, const Block & header_)
{
    std::lock_guard lock(mutex);
    auto iter = tables.find(table_id_);
    if (iter == tables.end())
    {
        LOG_TRACE(logger, "create new blocks table:{}-{}", session_id, table_id_);
        auto table = std::make_shared<TableStorage>(session_id, table_id_, header_);
        tables[table_id_] = table;
        return table;
    }

    auto & table = iter->second;
    const auto & table_header = table->getHeader();
    if (!blocksHaveEqualStructure(table_header, header_))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table({}-{}) exists with different header(), input header is :{}",
            session_id, table_id_, table_header.dumpNames(), header_.dumpNames());
    }
    return table;
}

void SessionHashedBlocksTablesStorage::releaseTable(const String & table_id_)
{
    LOG_INFO(logger, "release table {}.{}", session_id, table_id_);
    size_t table_count = 0;
    {
        std::lock_guard lock(mutex);
        tables.erase(table_id_);
        table_count = tables.size();
    }
    if (!table_count)
    {
        HashedBlocksStorage::getInstance().tryCloseSession(session_id);
    }
}

HashedBlocksStorage & HashedBlocksStorage::getInstance()
{
    static HashedBlocksStorage storage;
    return storage;
}

SessionHashedBlocksTablesStoragePtr HashedBlocksStorage::getSession(const String & session_id_) const
{
    std::lock_guard lock(mutex);
    auto iter = sessions.find(session_id_);
    if (iter == sessions.end())
    {
        LOG_INFO(logger, "Session() not found.", session_id_);
        return nullptr;
    }
    return iter->second;
}

SessionHashedBlocksTablesStoragePtr HashedBlocksStorage::getOrSetSession(const String & session_id_)
{
    std::lock_guard lock(mutex);
    auto iter = sessions.find(session_id_);
    if (iter == sessions.end())
    {
        LOG_TRACE(logger, "create new session:{}", session_id_);
        auto session = std::make_shared<SessionStorage>(session_id_);
        sessions[session_id_] = session;
        return session;
    }
    return iter->second;
}

void HashedBlocksStorage::closeSession(const String & session_id_)
{
    LOG_TRACE(logger, "close session:{}", session_id_);
    std::lock_guard lock(mutex);
    sessions.erase(session_id_);
}

void HashedBlocksStorage::tryCloseSession(const String & session_id_)
{
    std::lock_guard lock(mutex);
    auto iter = sessions.find(session_id_);
    if (iter == sessions.end())
    {
        LOG_TRACE(logger, "try to close a non-exists session:{}", session_id_);
        return;
    }
    auto & session = iter->second;
    std::lock_guard session_lock(session->getMutex());
    if (session->getTablesNumberWithoutMutex())
    {
        LOG_INFO(logger, "session({}) has tables which are in used", session_id_);
        return;
    }
    LOG_INFO(logger, "close session:{}", session_id_);
    sessions.erase(session_id_);
}


}
