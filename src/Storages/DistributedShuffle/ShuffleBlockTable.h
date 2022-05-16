#pragma once
#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <base/types.h>
#include <Poco/Glob.h>
#include <Poco/Logger.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
///
/// How to clear all the data when a query session has finished ?
/// The following measures were taken at current
/// 1）Chunks in ShuffleBlockTable are read only once, so we use popChunkWithoutMutex() for loading a chunk.
///   That ensures that all chunks are released after the loading finish.
/// 2) When ShuffleBlockTable becomes empty, it will call ShuffleBlockSession::releaseTable() to
///   release it-self.
/// 3) When ShuffleBlockSession becomes empty, it will call ShuffleBlockTableManager::tryCloseSession() to
///   release it-self.
/// All above will ensure all datas are released in normal processing. But more need be considered, exceptions could
/// happen during the processing which make the release actions not be called. Some measures may be token.
/// 1) In TCPHandler, catch all exceptions , and make a session releasing action on all nodes
/// 2) All sessions have a max TTL, make background routine to check timeout sessions and clear them.
///

class ShuffleBlockTable
{
public:
    using ChunkIterator = std::list<Chunk>::iterator;
    explicit ShuffleBlockTable(
        const String & session_id_,
        const String table_id_,
        const Block & header_)
        : session_id(session_id_)
        , table_id(table_id_)
        , header(header_)
    {}

    ~ShuffleBlockTable()
    {
        LOG_TRACE(logger, "close table {}.{}", session_id, table_id);
    }

    inline const Block & getHeader() const
    {
        return header;
    }
    inline const String & getSessionId() const { return session_id; }
    inline const String & getTableId() const { return table_id; }
    inline size_t getChunksNum() const { return chunks.size(); }


    // TODO : Should make merge action to reduce small size chunks?
    void addChunk(Chunk && chunk);

    Chunk popChunk();

    void makeSinkFinished()
    {
        is_sink_finished = true;
        LOG_INFO(logger, "{}.{} has total rows:{}", session_id, table_id, rows);
        wait_more_data.notify_all();
    }

private:
    std::mutex mutex;
    String session_id;
    String table_id;
    Block header;
    std::atomic<bool> is_sink_finished = false;
    std::list<Chunk> chunks;
    std::condition_variable wait_more_data;
    Poco::Logger * logger = &Poco::Logger::get("ShuffleBlockTable");
    size_t rows = 0;
};
using ShuffleBlockTablePtr = std::shared_ptr<ShuffleBlockTable>;

class ShuffleBlockSession
{
public:
    using Table = ShuffleBlockTable;
    using TablePtr = ShuffleBlockTablePtr;
    explicit ShuffleBlockSession(const String & session_id_, ContextPtr context_);

    const String & getSessionId() const { return session_id; }
    TablePtr getTable(const String & table_id_, bool wait_created = false);
    TablePtr getOrSetTable(const String & table_id_, const Block & header_);
    void releaseTable(const String & table_id_);

    size_t getTablesNumber() const
    {
        std::lock_guard lock{mutex};
        return tables.size();
    }

    bool isTimeout() const;

    void increaseRef() { ref_count += 1; }
    void decreaseRef();
    inline UInt32 getRefCount() const { return ref_count; }
    String dumpTables();
private:
    Poco::Logger * logger = &Poco::Logger::get("ShuffleBlockSession");
    String session_id;
    ContextPtr context;
    UInt64 created_timestamp;
    UInt64 timeout_second;
    mutable std::mutex mutex;
    std::condition_variable new_table_cond;
    std::unordered_map<String, std::shared_ptr<Table>> tables;
    std::atomic<UInt32> ref_count = 0;
};
using ShuffleBlockSessionPtr = std::shared_ptr<ShuffleBlockSession>;

class ShuffleBlockSessionHolder
{
public:
    ShuffleBlockSessionHolder() = default;
    explicit ShuffleBlockSessionHolder(ShuffleBlockSessionPtr session_);

    ~ShuffleBlockSessionHolder();

    ShuffleBlockSession & value() { return *session; }
    
private:
    ShuffleBlockSessionPtr session;

};

class ShuffleBlockTableManager : public boost::noncopyable
{
public:
    using Session = ShuffleBlockSession;
    using SessionPtr = ShuffleBlockSessionPtr;
    using SessionHolder = ShuffleBlockSessionHolder;

    static ShuffleBlockTableManager & getInstance();
    std::shared_ptr<SessionHolder> getSession(const String & session_id_) const;
    std::shared_ptr<SessionHolder> getOrSetSession(const String & session_id_, ContextPtr context_);

    void tryCloseSession(const String & session_id_);
protected:
    ShuffleBlockTableManager() = default;
private:
    Poco::Logger * logger = &Poco::Logger::get("ShuffleBlockTableManager");

    mutable std::mutex mutex;
    std::unordered_map<String, SessionPtr> sessions;

    void clearTimeoutSession();
};


}
