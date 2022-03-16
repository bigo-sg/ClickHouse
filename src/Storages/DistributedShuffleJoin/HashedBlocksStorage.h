#pragma once
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <base/types.h>
#include <Poco/Logger.h>

namespace DB
{
/**
 * FIXME : need lock protection ?
 * 
 */
class TableHashedBlocksStorage
{
public:
    using ChunkIterator = std::list<Chunk>::iterator;
    explicit TableHashedBlocksStorage(
        const String & session_id_,
        const String table_id_,
        const Block & header_)
        : session_id(session_id_)
        , table_id(table_id_)
        , header(header_)
    {}

    inline const Block & getHeader() const
    {
        return header;
    }
    inline const String & getSessionId() const { return session_id; }
    inline const String & getTableId() const { return table_id; }
    inline size_t getChunksNum() const { return chunks.size(); }


    // TODO : Should make merge action to reduce small size chunks?
    void addChunk(Chunk && chunk)
    {
        std::lock_guard lock(mutex);
        chunks.emplace_back(std::move(chunk));
    }

    ChunkIterator getChunksBegin()
    {
        return chunks.begin();
    }
    ChunkIterator getChunksEnd()
    {
        return chunks.end();
    }

    std::mutex & getMutex()
    {
        return mutex;
    }

private:
    std::mutex mutex;
    String session_id;
    String table_id;
    Block header;
    std::list<Chunk> chunks;
};
using TableHashedBlocksStoragePtr = std::shared_ptr<TableHashedBlocksStorage>;

class SessionHashedBlocksTablesStorage
{
public:
    using TableStorage = TableHashedBlocksStorage;
    using TableStoragePtr = TableHashedBlocksStoragePtr;

    explicit SessionHashedBlocksTablesStorage(const String & session_id_) : session_id(session_id_) {}

    TableStoragePtr getTable(const String & table_id_) const;
    TableStoragePtr getOrSetTable(const String & table_id_, const Block & header_);
    void releaseTable(const String & table_id_);
private:
    Poco::Logger * logger = &Poco::Logger::get("SessionHashedBlocksTablesStorage");
    String session_id;
    mutable std::mutex mutex;
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
protected:
    HashedBlocksStorage() = default;
private:
    Poco::Logger * logger = &Poco::Logger::get("HashedBlocksStorage");
    mutable std::mutex mutex;
    std::unordered_map<String, SessionStoragePtr> sessions;
};


}
