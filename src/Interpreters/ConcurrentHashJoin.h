#pragma once

#include <memory>
#include <Interpreters/HashJoin.h>
#include "Interpreters/ExpressionActions.h"
#include "Interpreters/IJoin.h"
#include <Common/Stopwatch.h>
namespace DB
{
namespace JoinStuff
{
class ConcurrentHashJoin : public IJoin
{
public:
    explicit ConcurrentHashJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, size_t slots_, const Block & right_sample_block, bool any_take_last_row_ = false);
    ~ConcurrentHashJoin() override;

    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    bool supportParallelJoin() const override { return true; }
    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override; 
private:
    struct InnerHashJoin
    {
        std::mutex mutex;
        std::unique_ptr<HashJoin> data;
    };
    ContextPtr context;
    std::shared_ptr<TableJoin> table_join;
    size_t slots;
    std::vector<std::shared_ptr<InnerHashJoin>> hash_joins;

    Block totals;

    enum TableIndex 
    {
        LEFT = 0,
        RIHT = 1
    };

    struct BlockDispatchData
    {
        std::mutex mutex;
        std::atomic<bool> has_init = false;
        std::shared_ptr<ExpressionActions> hash_expression_actions;
        Strings hash_columns_names;
        Block header;
        BlockDispatchData() = default;
    };

    std::vector<std::shared_ptr<BlockDispatchData>> dispatch_datas;

    Poco::Logger * logger = &Poco::Logger::get("ConcurrentHashJoin");

    std::atomic<size_t> make_dispatched_blocks_elapsed = 0;
    //Stopwatch make_dispatched_blocks_watch;
    std::atomic<size_t> insert_right_blocks_elapsed = 0;
    //Stopwatch insert_right_blocks_watch;
    std::atomic<size_t> insert_left_blocks_elapsed = 0;
    //Stopwatch insert_left_blocks_watch;
    std::atomic<size_t> merge_left_blocks_elapsed = 0;
    //Stopwatch merge_left_blocks_watch;

    std::shared_ptr<ExpressionActions> buildHashExpressionAction(const Block & block, const String & based_column_name, Strings & hash_columns_names);
    BlockDispatchData & getBlockDispatchData(const Block & block, TableIndex table_index);

    void dispatchBlock(BlockDispatchData & dispatch_data, Block & from_block, std::vector<Block> & dispatched_blocks);

};
}
}
