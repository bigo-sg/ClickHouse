#pragma once
#include <optional>
#include <Processors/Formats/IInputFormat.h>
#include <Interpreters/Context_fwd.h>
#include <substrait/plan.pb.h>
#include <Poco/URI.h>
#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <vector>
#include <memory>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>

namespace local_engine
{
class FormatFile
{
public:
    FormatFile(DB::ContextPtr context_, const String & uri_path_, ReadBufferBuilderPtr read_buffer_builder_);
    virtual ~FormatFile() = default;

    /// create a new input format for reading this file
    virtual std::pair<DB::InputFormatPtr, std::unique_ptr<DB::ReadBuffer>> createInputFormat(const DB::Block & header) = 0;

    /// try to get rows from file metadata
    virtual std::optional<size_t> getTotalRows() { return {}; }

    /// get partition keys from file path
    inline const std::vector<String> & getFilePartitionKeys() const { return partition_keys; }

    inline const std::map<String, String> & getFilePartitionValues() const { return partition_values; }

    inline String getURIPath() const { return uri_path; }

protected:
    DB::ContextPtr context;
    String uri_path;
    ReadBufferBuilderPtr read_buffer_builder;
    std::vector<String> partition_keys;
    std::map<String, String> partition_values;

};
using FormatFilePtr = std::shared_ptr<FormatFile>;
using FormatFiles = std::vector<FormatFilePtr>;

class FormatFileUtil
{
public:
    static FormatFilePtr createFile(DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file);
};
}
