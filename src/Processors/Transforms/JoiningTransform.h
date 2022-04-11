#pragma once
#include <memory>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>
#include "base/types.h"


namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class NotJoinedBlocks;

class ScopedWatch
{
public:
    explicit ScopedWatch(Stopwatch & watch_, UInt64 & counter_, UInt64 & elapsed_)
        : watch(watch_)
        , counter(counter_)
        , elapsed(elapsed_)
    {
        watch.start();
        counter++;
    }
    ~ScopedWatch()
    {
        elapsed += watch.elapsedMilliseconds();
    }
private:
    Stopwatch & watch;
    UInt64 & counter;
    UInt64 & elapsed;
};

class ScopedRestartWatch
{
public:
    explicit ScopedRestartWatch(Stopwatch & watch_):watch(watch_){}
    ~ScopedRestartWatch(){ watch.start(); }
private:
    Stopwatch & watch;
};
/// Join rows to chunk form left table.
/// This transform usually has two input ports and one output.
/// First input is for data from left table.
/// Second input has empty header and is connected with FillingRightJoinSide.
/// We can process left table only when Join is filled. Second input is used to signal that FillingRightJoinSide is finished.
class JoiningTransform : public IProcessor
{
public:

    /// Count streams and check which is last.
    /// The last one should process non-joined rows.
    class FinishCounter
    {
    public:
        explicit FinishCounter(size_t total_) : total(total_) {}

        bool isLast()
        {
            return finished.fetch_add(1) + 1 >= total;
        }

    private:
        const size_t total;
        std::atomic<size_t> finished{0};
    };

    using FinishCounterPtr = std::shared_ptr<FinishCounter>;

    JoiningTransform(
        Block input_header,
        JoinPtr join_,
        size_t max_block_size_,
        bool on_totals_ = false,
        bool default_totals_ = false,
        FinishCounterPtr finish_counter_ = nullptr);

    ~JoiningTransform() override;

    String getName() const override { return "JoiningTransform"; }

    static Block transformHeader(Block header, const JoinPtr & join);

    Status prepare() override;
    void work() override;

protected:
    void transform(Chunk & chunk);

private:
    Chunk input_chunk;
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;
    bool stop_reading = false;
    bool process_non_joined = true;

    JoinPtr join;
    bool on_totals;
    /// This flag means that we have manually added totals to our pipeline.
    /// It may happen in case if joined subquery has totals, but out string doesn't.
    /// We need to join default values with subquery totals if we have them, or return empty chunk is haven't.
    bool default_totals;
    bool initialized = false;

    ExtraBlockPtr not_processed;

    FinishCounterPtr finish_counter;
    std::shared_ptr<NotJoinedBlocks> non_joined_blocks;
    size_t max_block_size;

    std::unique_ptr<Stopwatch> watch;
    UInt64 wait_right_table_elapsed = 0;

    Stopwatch work_watch;
    UInt64 work_elapsed = 0;
    UInt64 work_count = 0;

    Stopwatch prepare_watch;
    UInt64 prepare_elapsed = 0;
    UInt64 prepare_count = 0;

    Stopwatch wait_prepare_watch;
    UInt64 wait_prepare_elapsed = 0;

    Stopwatch wait_work_watch;
    UInt64 wait_work_elapsed = 0;

    UInt64 input_rows = 0;

    Block readExecute(Chunk & chunk);
};

/// Fills Join with block from right table.
/// Has single input and single output port.
/// Output port has empty header. It is closed when all data is inserted in join.
class FillingRightJoinSideTransform : public IProcessor
{
public:
    FillingRightJoinSideTransform(Block input_header, JoinPtr join_);
    ~FillingRightJoinSideTransform() override;
    String getName() const override { return "FillingRightJoinSide"; }

    InputPort * addTotalsPort();

    Status prepare() override;
    void work() override;

private:
    JoinPtr join;
    Chunk chunk;
    bool stop_reading = false;
    bool for_totals = false;
    bool set_totals = false;
    Stopwatch work_watch;
    UInt64 work_elapsed = 0;
    UInt64 work_count = 0;
    Stopwatch prepare_watch;
    UInt64 prepare_elapsed = 0;
    UInt64 prepare_count = 0;
    UInt64 input_rows = 0;

    Stopwatch wait_prepare_watch;
    UInt64 wait_prepare_elapsed = 0;

    Stopwatch wait_work_watch;
    UInt64 wait_work_elapsed = 0;
};

}
