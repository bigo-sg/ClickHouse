#include <memory>
#include <mutex>
#include <Storages/DistributedShuffleJoin/HashedBlocksStorage.h>
#include <base/logger_useful.h>
#include "Common/Exception.h"
#include <Common/ErrorCodes.h>
#include "base/defines.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void TableHashedBlocksStorage::addChunk(Chunk && chunk)
{
    if (likely(chunk.hasRows()))
    {
        LOG_TRACE(logger, "{}.{} add chunk. rows:{}", session_id, table_id, chunk.getNumRows());
        std::unique_lock lock(mutex);
        chunks.emplace_back(std::move(chunk));
    }
    else
    {
        LOG_TRACE(logger, "add empty chunk");
    }
    wait_more_data.notify_one();
}

Chunk TableHashedBlocksStorage::popChunk()
{
    std::unique_lock lock(mutex);
    while(!isSinkFinished() && chunks.empty())
    {
        wait_more_data.wait(lock, [&] { return isSinkFinished() || !chunks.empty(); });
    }
    LOG_TRACE(logger, "popChunk. isSinkFinished()={}, chunks.size()={}", isSinkFinished(), chunks.size());

    Chunk res;
    if (likely(!chunks.empty()))
    {
        res.swap(chunks.front());
        chunks.pop_front();
        if (unlikely(!res.hasRows()))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should not be empty");
        }
    }
    lock.unlock();
    return res;
}

void TableHashedBlocksStorage::increaseFinishedSinkCount()
{
    finshed_sink_count++;
    if (finshed_sink_count >= active_sinks)
    {
        LOG_INFO(logger, "sinking into table({}.{}) finished. sinks:{}", session_id, table_id, active_sinks);
    }
    if (finshed_sink_count > active_sinks)
    {
        // It is based on the truth that each node would only start one sink for each table
        // If this was broken, we cannot use this too judge whether the sinking phase finish.
        // Need to be careful with what happen in InterpreterInsertQuery
        // Otherwise we need to use the block mode in TreeQueryTransform
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table({}.{}), only {} sinks are expected. but we get {} now.", session_id, table_id, active_sinks, finshed_sink_count);
    }
    wait_more_data.notify_all();
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

TableHashedBlocksStoragePtr SessionHashedBlocksTablesStorage::getTable(const String & table_id_, bool wait_created)
{
    std::unique_lock lock(mutex);
    auto iter = tables.find(table_id_);
    if (iter == tables.end())
    {
        if (!wait_created)
        {
            LOG_INFO(logger, "Table({}) not found in session({})", table_id_, session_id);
            return nullptr;
        }
        new_table_cond.wait(lock, [&]{iter = tables.find(table_id_); return iter != tables.end();});
    }
    return iter->second;
}

TableHashedBlocksStoragePtr SessionHashedBlocksTablesStorage::getOrSetTable(const String & table_id_, const Block & header_, UInt64 active_sinks_)
{
    TableHashedBlocksStoragePtr table;
    bool is_new_table = false;
    {
        std::lock_guard lock(mutex);
        auto iter = tables.find(table_id_);
        if (iter == tables.end())
        {
            LOG_TRACE(logger, "create new blocks table:{}.{}", session_id, table_id_);
            table = std::make_shared<TableStorage>(session_id, table_id_, header_, active_sinks_);
            tables[table_id_] = table;
            is_new_table = true;
        }
        else
        {
            table = iter->second;
        }
    }
    if (is_new_table)
    {
        new_table_cond.notify_all();
        return table;
    }

    const auto & table_header = table->getHeader();
    if (!blocksHaveEqualStructure(table_header, header_) || (table->getActiveSinks() != active_sinks_))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Table({}-{}) exists with different header(), input header is :{}",
            session_id,
            table_id_,
            table_header.dumpNames(),
            header_.dumpNames());
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
