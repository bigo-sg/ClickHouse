#pragma once
#include <memory>
#include <Storages/SubstraitSource/FormatFile.h>
#include <parquet/arrow/reader.h>
namespace local_engine
{
class ParquetFormatFile : public FormatFile
{
public:
    explicit ParquetFormatFile(DB::ContextPtr context_, const String & uri_path_, ReadBufferBuilderPtr read_buffer_builder_);
    ~ParquetFormatFile() override = default;
    std::pair<DB::InputFormatPtr, std::unique_ptr<DB::ReadBuffer>> createInputFormat(const DB::Block & header) override;
    std::optional<size_t> getTotalRows() override;

private:
    std::mutex mutex;
    std::optional<size_t> total_rows;

    std::unique_ptr<parquet::arrow::FileReader> reader;
    void prepareReader();
};

}
