#include <memory>
#include <mutex>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DistributedShuffle/ShuffleBlockTable.h>
#include <base/defines.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Poco/Exception.h>
#include <Poco/Logger.h>
#include <Poco/Timestamp.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event ClearTimeoutShuffleStorageSession;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void ShuffleBlockTable::addChunk(Chunk && chunk)
{
    if (chunk.hasRows()) [[likely]]
    {
        {
            std::unique_lock lock(mutex);
            if (is_sink_finished) [[unlikely]]
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Try in insert into a sink finished table({}.{})", session_id, table_id);
            while (remained_rows > max_rows_limit)
                wait_consume_data.wait(lock);
            rows += chunk.getNumRows();
            remained_rows += chunk.getNumRows();
            if (remained_rows > max_rows)
                max_rows = remained_rows;
            chunks.emplace_back(std::move(chunk));
        }
        wait_more_data.notify_one();
    }
    else
    {
        LOG_TRACE(logger, "Add an empty chunk. table({}.{})", session_id, table_id);
        wait_more_data.notify_all();
    }
}

Chunk ShuffleBlockTable::popChunk()
{
    Chunk res;
    {
        std::unique_lock lock(mutex);
        while (chunks.empty())
        {
            if (!is_sink_finished)
            {
                wait_more_data.wait(lock, [&] { return is_sink_finished || !chunks.empty(); });
            }
            else
            {
                break;
            }
        }
        //LOG_TRACE(logger, "{}.{} popChunk. isSinkFinished()={}, chunks.size()={}", session_id, table_id, is_sink_finished, chunks.size());

        if (!chunks.empty()) [[likely]]
        {
            res.swap(chunks.front());
            remained_rows -= res.getNumRows();
            chunks.pop_front();
            if (unlikely(!res.hasRows()))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should not be empty. table({}.{})", session_id, table_id);
            }
        }
    }
    wait_consume_data.notify_all();
    return res;
}

ShuffleBlockSession::ShuffleBlockSession(const String & session_id_, ContextPtr context_) : session_id(session_id_), context(context_)
{
    created_timestamp = Poco::Timestamp().raw()/1000000;
    timeout_second = context->getSettings().shuffle_storage_session_timeout;
}

ShuffleBlockTablePtr ShuffleBlockSession::getTable(const String & table_id_, bool wait_created)
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

ShuffleBlockTablePtr ShuffleBlockSession::getOrSetTable(const String & table_id_, const Block & header_)
{
    ShuffleBlockTablePtr table;
    bool is_new_table = false;
    {
        std::lock_guard lock(mutex);
        auto iter = tables.find(table_id_);
        if (iter == tables.end())
        {
            LOG_TRACE(logger, "create new blocks table:{}.{}", session_id, table_id_);
            table = std::make_shared<Table>(session_id, table_id_, header_);
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
    if (!blocksHaveEqualStructure(table_header, header_))
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

void ShuffleBlockSession::releaseTable(const String & table_id_)
{
    LOG_INFO(logger, "release table {}.{}", session_id, table_id_);
    {
        std::lock_guard lock(mutex);
        auto iter = tables.find(table_id_);
        if (iter != tables.end())
        {
            iter->second->makeSinkFinished();
        }
        tables.erase(table_id_);
    }
}

bool ShuffleBlockSession::isTimeout() const
{
    UInt64 now = Poco::Timestamp().raw()/1000000;
    return (created_timestamp + timeout_second < now);
}

void ShuffleBlockSession::decreaseRef()
{
    if (ref_count == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session({}) ref_count = 0", session_id);
    ref_count -= 1;
}

String ShuffleBlockSession::dumpTables()
{
    std::lock_guard lock(mutex);
    String table_names;
    int i = 0;
    for (auto & table : tables)
    {
        if (i)
            table_names += ",";
        i += 1;
        table_names += table.first;
    }
    return table_names;
}

ShuffleBlockTableManager & ShuffleBlockTableManager::getInstance()
{
    static ShuffleBlockTableManager storage;
    return storage;
}

std::shared_ptr<ShuffleBlockSessionHolder> ShuffleBlockTableManager::getSession(const String & session_id_) const
{
    std::lock_guard lock(mutex);

    auto iter = sessions.find(session_id_);
    if (iter == sessions.end())
    {
        LOG_INFO(logger, "Session({}) not found.", session_id_);
        return nullptr;
    }
    iter->second->increaseRef();
    return std::make_shared<ShuffleBlockSessionHolder>(iter->second);
}

std::shared_ptr<ShuffleBlockSessionHolder> ShuffleBlockTableManager::getOrSetSession(const String & session_id_, ContextPtr context_)
{
    std::lock_guard lock(mutex);
    clearTimeoutSession();

    auto iter = sessions.find(session_id_);
    if (iter == sessions.end())
    {
        LOG_TRACE(logger, "create new session:{}", session_id_);
        auto session = std::make_shared<Session>(session_id_, context_);
        session->increaseRef();
        sessions[session_id_] = session;
        return std::make_shared<ShuffleBlockSessionHolder>(session);
    }
    iter->second->increaseRef();
    return std::make_shared<ShuffleBlockSessionHolder>(iter->second);
}

ShuffleBlockSessionHolder::ShuffleBlockSessionHolder(ShuffleBlockSessionPtr session_)
    : session(session_)
{
}

ShuffleBlockSessionHolder::~ShuffleBlockSessionHolder()
{
}


void ShuffleBlockTableManager::tryCloseSession(const String & session_id_)
{
    std::lock_guard lock(mutex);
    auto iter = sessions.find(session_id_);
    if (iter == sessions.end())
    {
        LOG_TRACE(logger, "try to close a non-exists session:{}", session_id_);
        return;
    }
    auto & session = iter->second;
    session->decreaseRef();
    if (session->getRefCount() || session->getTablesNumber())
    {
        LOG_INFO(logger, "session({}) is in used. ref={}, tables={}", session_id_, session->getRefCount(), session->dumpTables());
        return;
    }
    LOG_INFO(logger, "close session:{}", session_id_);
    sessions.erase(session_id_);
}

void ShuffleBlockTableManager::clearTimeoutSession()
{
    for (auto it = sessions.begin(); it != sessions.end();)
    {
        if (it->second->isTimeout())
        {
            LOG_TRACE(&Poco::Logger::get("ShuffleBlockTableManager"), "Clear timeoout session: {}", it->first);
            ProfileEvents::increment(ProfileEvents::ClearTimeoutShuffleStorageSession, 1);
            sessions.erase(it++);
        }
        else
            it++;
    }
}


}
