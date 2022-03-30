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
#include <base/logger_useful.h>

namespace DB
{
/**
 * How to clear all the data when a query session has finished ?
 * The following measures were taken at current
 * 1）Chunks in TableHashedBlocksStorage are read only once, so we use popChunkWithoutMutex() for loading a chunk.
 *   That ensures that all chunks are released after the loading finish.
 * 2) When TableHashedBlocksStorage becomes empty, it will call SessionHashedBlocksTablesStorage::releaseTable() to 
 *   release it-self.
 * 3) When SessionHashedBlocksTablesStorage becomes empty, it will call HashedBlocksStorage::tryCloseSession() to 
 *   release it-self.
 * All above will ensure all datas are released in normal processing. But more need be considered, exceptions could
 * happen during the processing which make the release actions not be called. Some measures may be token.
 * 1) In TCPHandler, catch all exceptions , and make a session releasing action on all nodes
 * 2) All sessions have a max TTL, make background routine to check timeout sessions and clear them.
 * 
 */
class TableHashedBlocksStorage
{
public:
    using ChunkIterator = std::list<Chunk>::iterator;
    explicit TableHashedBlocksStorage(
        const String & session_id_,
        const String table_id_,
        const Block & header_,
        UInt64 active_sinks_
        )
        : session_id(session_id_)
        , table_id(table_id_)
        , header(header_)
        , active_sinks(active_sinks_)
        , finshed_sink_count(0)
    {}

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

    ChunkIterator getChunksBegin()
    {
        return chunks.begin();
    }
    ChunkIterator getChunksEnd()
    {
        return chunks.end();
    }

    inline size_t getChunkSizeWithoutMutex()
    {
        return chunks.size();
    }

    Chunk popChunkWithoutMutex();

    std::mutex & getMutex()
    {
        return mutex;
    }

    void increaseFinishedSinkCount();

    inline bool isSinkFinished() const
    {
        return finshed_sink_count >= active_sinks;
    }

    inline UInt64 getActiveSinks() const { return active_sinks; }
private:
    std::mutex mutex;
    String session_id;
    String table_id;
    Block header;
    UInt64 active_sinks;
    std::atomic_uint64_t finshed_sink_count;
    std::list<Chunk> chunks;
    std::condition_variable wait_more_data;
    Poco::Logger * logger = &Poco::Logger::get("TableHashedBlocksStorage");
};
using TableHashedBlocksStoragePtr = std::shared_ptr<TableHashedBlocksStorage>;

class SessionHashedBlocksTablesStorage
{
public:
    using TableStorage = TableHashedBlocksStorage;
    using TableStoragePtr = TableHashedBlocksStoragePtr;

    explicit SessionHashedBlocksTablesStorage(const String & session_id_) : session_id(session_id_) {}

    TableStoragePtr getTable(const String & table_id_, bool wait_created = false);
    TableStoragePtr getOrSetTable(const String & table_id_, const Block & header_, UInt64 active_sinks_ = 0);
    void releaseTable(const String & table_id_);
    std::mutex & getMutex()
    {
        return mutex;
    }
    size_t getTablesNumberWithoutMutex() const
    {
        return tables.size();
    }
private:
    Poco::Logger * logger = &Poco::Logger::get("SessionHashedBlocksTablesStorage");
    String session_id;
    mutable std::mutex mutex;
    std::condition_variable new_table_cond;
    std::unordered_map<String, std::shared_ptr<TableStorage>> tables;
};
using SessionHashedBlocksTablesStoragePtr = std::shared_ptr<SessionHashedBlocksTablesStorage>;

class HashedBlocksStorage : public boost::noncopyable
{
public:
    using SessionStorage = SessionHashedBlocksTablesStorage;
    using SessionStoragePtr = SessionHashedBlocksTablesStoragePtr;

    static HashedBlocksStorage & getInstance();
    SessionStoragePtr getSession(const String & session_id_) const;
    SessionStoragePtr getOrSetSession(const String & session_id_);

    void closeSession(const String & session_id_);
    void tryCloseSession(const String & session_id_);
protected:
    HashedBlocksStorage() = default;
private:
    Poco::Logger * logger = &Poco::Logger::get("HashedBlocksStorage");
    mutable std::mutex mutex;
    std::unordered_map<String, SessionStoragePtr> sessions;
};


}
