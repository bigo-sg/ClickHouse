#include <memory>
#include <mutex>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/TableJoin.h>
#include "Columns/FilterDescription.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Core/NamesAndTypes.h"
#include "IO/WriteBufferFromString.h"
#include "Interpreters/ActionsDAG.h"
#include "Interpreters/ActionsVisitor.h"
#include "Interpreters/ExpressionActions.h"
#include "Interpreters/PreparedSets.h"
#include "Interpreters/SubqueryForSet.h"
#include "Parsers/DumpASTNode.h"
#include "Parsers/ExpressionListParsers.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/parseQuery.h"
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include "Common/Exception.h"
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}
namespace JoinStuff
{
ConcurrentHashJoin::ConcurrentHashJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, size_t slots_, const Block & right_sample_block, bool any_take_last_row_)
    : context(context_)
    , table_join(table_join_)
    , slots(slots_)
{
    if (!slots_)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid argument slot : {}", slots_);
    }
    for (size_t i = 0; i < slots; ++i)
    {
        auto inner_hash_join = std::make_shared<InnerHashJoin>();
        inner_hash_join->data = std::make_unique<HashJoin>(table_join_, right_sample_block, any_take_last_row_);
        hash_joins.emplace_back(std::move(inner_hash_join));
    }
    dispatch_datas.emplace_back(std::make_shared<BlockDispatchData>());
    dispatch_datas.emplace_back(std::make_shared<BlockDispatchData>());
}

ConcurrentHashJoin::~ConcurrentHashJoin()
{
    LOG_TRACE(logger, "elapsed. make_dispatched_blocks_elapsed:{}, insert_right_blocks_elapsed:{}, insert_left_blocks_elapsed:{}, merge_left_blocks_elapsed:{}",
        make_dispatched_blocks_elapsed, insert_right_blocks_elapsed, insert_left_blocks_elapsed, merge_left_blocks_elapsed);
}

bool ConcurrentHashJoin::addJoinedBlock(const Block & block, bool check_limits)
{
    LOG_TRACE(logger, "addJoinedBlock. rows:{}", block.rows());
    auto & dispatch_data = getBlockDispatchData(block, RIHT);
    std::vector<Block> dispatched_blocks;
    Block cloned_block = block;
    dispatchBlock(dispatch_data, cloned_block, dispatched_blocks);
    Stopwatch insert_right_blocks_watch;
    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {   
        auto & hash_join = hash_joins[i];
        auto & dispatched_block = dispatched_blocks[i];
        std::lock_guard lock(hash_join->mutex);
        hash_join->data->addJoinedBlock(dispatched_block, false);
    }
    insert_right_blocks_elapsed += insert_right_blocks_watch.elapsedMicroseconds();
    if (check_limits)
        return table_join->sizeLimits().check( getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    LOG_TRACE(logger, "joinBlock. rows:{}", block.rows());
    auto & dispatch_data = getBlockDispatchData(block, LEFT);
    std::vector<Block> dispatched_blocks;
    Block cloned_block = block;
    dispatchBlock(dispatch_data, cloned_block, dispatched_blocks);
    Stopwatch insert_left_blocks_watch;
    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {
        std::shared_ptr<ExtraBlock> none_extra_block;
        auto & hash_join = hash_joins[i];
        auto & dispatched_block = dispatched_blocks[i];
        //std::lock_guard lock(hash_join->mutex);
        hash_join->data->joinBlock(dispatched_block, none_extra_block);
        if (none_extra_block && !none_extra_block->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not_processed should be empty");
    }
    insert_left_blocks_elapsed += insert_left_blocks_watch.elapsedMicroseconds();

    Stopwatch merge_left_blocks_watch;
    ColumnsWithTypeAndName final_columns;
    MutableColumns mutable_final_columns;
    NamesAndTypesList names_and_types = dispatched_blocks[0].getNamesAndTypesList();
    auto types = names_and_types.getTypes();
    for (auto & dispatched_block : dispatched_blocks)
    {
        for (size_t pos = 0; pos < dispatched_block.columns(); ++pos)
        {
            auto & from_column = dispatched_block.getByPosition(pos);
            if (mutable_final_columns.size() <= pos)
            {
                mutable_final_columns.emplace_back(from_column.column->cloneEmpty());
                //mutable_final_columns.emplace_back(types[pos]->createColumn());
            }
            if (!from_column.column->empty())
            {
                mutable_final_columns[pos]->insertRangeFrom(*from_column.column, 0, from_column.column->size());
            }
        }
    }

    size_t i = 0;
    for (auto & name_and_type : names_and_types)
    {
        ColumnPtr col_ptr = std::move(mutable_final_columns[i]);
        mutable_final_columns[i] = nullptr;
        ColumnWithTypeAndName col(col_ptr, name_and_type.type, name_and_type.name);
        final_columns.emplace_back(col);
        i += 1;
    }
    block = Block(final_columns);
    merge_left_blocks_elapsed += merge_left_blocks_watch.elapsedMicroseconds();
}

void ConcurrentHashJoin::checkTypesOfKeys(const Block & block) const
{
    hash_joins[0]->data->checkTypesOfKeys(block);
}

void ConcurrentHashJoin::setTotals(const Block & block)
{
    totals = block;
}

const Block & ConcurrentHashJoin::getTotals() const
{
    return totals;
}

size_t ConcurrentHashJoin::getTotalRowCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        res += hash_join->data->getTotalRowCount();
    }
    return res;
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        res += hash_join->data->getTotalByteCount();
    }
    return res;
}

bool ConcurrentHashJoin::alwaysReturnsEmptySet() const
{
    for (const auto & hash_join : hash_joins)
    {
        if (!hash_join->data->alwaysReturnsEmptySet())
            return false;
    }
    return true;
}

std::shared_ptr<NotJoinedBlocks> ConcurrentHashJoin::getNonJoinedBlocks(
        const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    if (table_join->strictness() == ASTTableJoin::Strictness::Asof ||
        table_join->strictness() == ASTTableJoin::Strictness::Semi ||
        !isRightOrFull(table_join->kind()))
    {
        return {};
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid join type. join kind: {}, strictness: {}", table_join->kind(), table_join->strictness());
}

std::shared_ptr<ExpressionActions> ConcurrentHashJoin::buildHashExpressionAction(const Block & block, const String & based_column_name, Strings & hash_columns_names)
{
    WriteBufferFromOwnString write_buf;
    for (size_t i = 0; i < slots; ++i)
    {
        if (i)
            write_buf << ",";
        write_buf << "cityHash64(" << based_column_name << ")%" << slots << "=" << i;
    }
    auto settings = context->getSettings();
    ParserExpressionList hash_expr_parser(true);
    ASTPtr func_ast = parseQuery(hash_expr_parser, write_buf.str(), "Parse Block hash expression", settings.max_query_size, settings.max_parser_depth);
    for (auto & child : func_ast->children)
        hash_columns_names.emplace_back(child->getColumnName());
    
    DebugASTLog<false> visit_log;
    const auto & names_and_types = block.getNamesAndTypesList();
    ActionsDAGPtr actions = std::make_shared<ActionsDAG>(names_and_types);
    PreparedSets prepared_sets;
    SubqueriesForSets subqueries_for_sets;
    ActionsVisitor::Data visitor_data(
        context,
        SizeLimits{settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode},
        10,
        names_and_types,
        std::move(actions),
        prepared_sets,
        subqueries_for_sets,
        true, false, true, false);
    ActionsVisitor(visitor_data, visit_log.stream()).visit(func_ast);
    actions = visitor_data.getActions();
    return std::make_shared<ExpressionActions>(actions);
}

ConcurrentHashJoin::BlockDispatchData & ConcurrentHashJoin::getBlockDispatchData(const Block & block, TableIndex table_index)
{
    auto & data = *dispatch_datas[table_index];
    if (data.has_init)
        return data;
    std::lock_guard lock(data.mutex);
    if (data.has_init)
        return data;
    
    if (table_join->getClauses().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "empty join clauses");
    const auto & onexpr = table_join->getClauses()[0];
    if (table_index == LEFT)
    {
        const auto based_column_name = onexpr.key_names_left[0];
        data.hash_expression_actions = buildHashExpressionAction(block, based_column_name, data.hash_columns_names);
    }
    else 
    {
        const auto based_column_name = onexpr.key_names_right[0];
        data.hash_expression_actions = buildHashExpressionAction(block, based_column_name, data.hash_columns_names);
    }
    data.header = block.cloneEmpty();
    data.has_init = true;
    return data;
}

void ConcurrentHashJoin::dispatchBlock(BlockDispatchData & dispatch_data, Block & from_block, std::vector<Block> & dispatched_blocks)
{
    Stopwatch make_dispatched_blocks_watch;
    auto rows_before_filtration = from_block.rows();
    dispatch_data.hash_expression_actions->execute(from_block, rows_before_filtration);
    for (const auto & filter_column_name : dispatch_data.hash_columns_names)
    {
        auto full_column = from_block.findByName(filter_column_name)->column->convertToFullColumnIfConst();
        auto filter_desc = std::make_unique<FilterDescription>(*full_column);
        auto num_filtered_rows = filter_desc->countBytesInFilter();
        ColumnsWithTypeAndName filtered_block_columns;
        for (size_t i = 0; i < dispatch_data.header.columns(); ++i)
        {
            auto & from_column = from_block.getByPosition(i);
            auto filtered_column = filter_desc->filter(*from_column.column, num_filtered_rows);
            filtered_block_columns.emplace_back(filtered_column, from_column.type, from_column.name);
        }
        dispatched_blocks.emplace_back(filtered_block_columns);
    }
    make_dispatched_blocks_elapsed += make_dispatched_blocks_watch.elapsedMicroseconds();
}
}
}
