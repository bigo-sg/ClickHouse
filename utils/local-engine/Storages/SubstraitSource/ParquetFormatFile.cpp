#include <memory>
#include <utility>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/ArrowParquetBlockInputFormat.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
ParquetFormatFile::ParquetFormatFile(DB::ContextPtr context_, const String & uri_path_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, uri_path_, read_buffer_builder_)
{}

FormatFile::InputFormatPtr ParquetFormatFile::createInputFormat(const DB::Block & header)
{
    auto read_buffer = read_buffer_builder->build(uri_path);
    auto input_format = std::make_shared<local_engine::ArrowParquetBlockInputFormat>(*read_buffer, header, DB::FormatSettings());
    auto res = std::make_shared<FormatFile::InputFormat>(input_format, std::move(read_buffer));
    return res;
}

std::optional<size_t> ParquetFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }
    prepareReader();
    auto meta = reader->parquet_reader()->metadata();
    {
        std::lock_guard lock(mutex);
        total_rows = meta->num_rows();
        return total_rows;
    }
}

void ParquetFormatFile::prepareReader()
{
    std::lock_guard lock(mutex);
    if (reader)
        return;
    auto in = read_buffer_builder->build(uri_path);
    DB::FormatSettings format_settings;
    format_settings.seekable_read = true;
    std::atomic<int> is_stopped{0};
    auto status = parquet::arrow::OpenFile(
        asArrowFile(*in, format_settings, is_stopped), arrow::default_memory_pool(), &reader);
    if (!status.ok())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Open file({}) failed. {}", uri_path, status.ToString());
    }
}
}
