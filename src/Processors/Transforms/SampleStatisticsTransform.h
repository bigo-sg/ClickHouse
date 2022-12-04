#pragma once

#include <memory>
#include <mutex>
#include <Analyzer/BlockStatAnalyzer.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <QueryPipeline/Pipe.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
class SampleStatisticsTransform : public IProcessor
{
public:
    enum ProcessStatus
    {
        BEFORE_SAMPLING,
        DO_SAMPLING,
        WAIT_SAMPLED,
        AFTER_SAMPLING,
    };

    struct SharedState
    {
        std::mutex mutex;
        std::atomic<ProcessStatus> process_status = BEFORE_SAMPLING;
        std::shared_ptr<BlockStatMetadata> stat = nullptr;
    };

    explicit SampleStatisticsTransform(const Block & header_, std::shared_ptr<SharedState> shared_state_, size_t sample_rows_num_ = 4096);
    ~SampleStatisticsTransform() override = default;
    
    String getName() const override {return "SampleStatisticsTransform"; }
    Status prepare() override;
    void work() override;

private:
    Block header;
    std::shared_ptr<SharedState> shared_state;
    size_t sample_rows_num;
    size_t has_sample_rows = 0;
    ProcessStatus local_status;
    std::list<Chunk> sampled_chunks;
    
    bool input_finished = false;
    bool has_input = false;
    bool has_output = false;
    Chunk output_chunk;

    Poco::Logger * logger = &Poco::Logger::get("SampleStatisticsTransform");
    
    Status normalPrepare(InputPort & input);
    Status samplingPrepare(InputPort & input);

    void normalWork();
    void samplingWork();

};
}
