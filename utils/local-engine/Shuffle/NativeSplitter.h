#pragma once
#include <jni.h>
#include <mutex>
#include <stack>
#include <Shuffle/ShuffleSplitter.h>
#include "Common/ActionLock.h"
#include <Common/BlockIterator.h>
#include "Columns/ColumnLowCardinality.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/NamesAndTypes.h"
#include "Interpreters/Context_fwd.h"
#include "Processors/Chunk.h"
#include "base/types.h"
#include <Core/SortDescription.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>

namespace local_engine
{
class NativeSplitter : BlockIterator
{
public:
    struct Options
    {
        size_t buffer_size = 8192;
        size_t partition_nums;
        std::string exprs_buffer;
        //std::vector<std::string> exprs;
    };

    struct Holder
    {
        std::unique_ptr<NativeSplitter> splitter = nullptr;
    };

    static jclass iterator_class;
    static jmethodID iterator_has_next;
    static jmethodID iterator_next;
    static std::unique_ptr<NativeSplitter> create(const std::string & short_name, Options options, jobject input);

    NativeSplitter(Options options, jobject input);
    bool hasNext();
    DB::Block * next();
    int32_t nextPartitionId();


    virtual ~NativeSplitter();

protected:
    virtual void computePartitionId(DB::Block &) { }
    Options options;
    std::vector<DB::IColumn::ColumnIndex> partition_ids;


private:
    void split(DB::Block & block);
    int64_t inputNext();
    bool inputHasNext();


    std::vector<std::shared_ptr<ColumnsBuffer>> partition_buffer;
    std::stack<std::pair<int32_t, std::unique_ptr<DB::Block>>> output_buffer;
    int32_t next_partition_id = -1;
    jobject input;
    
    std::atomic<UInt64> total_time = 0;
    std::atomic<UInt64> begin_ts = 0;
    std::atomic<UInt64> total_rows = 0;
};

class HashNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;

public:
    HashNativeSplitter(NativeSplitter::Options options_, jobject input);

private:
    std::vector<std::string> hash_fields;
    DB::FunctionBasePtr hash_function;
};

class RoundRobinNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;

public:
    RoundRobinNativeSplitter(NativeSplitter::Options options_, jobject input) : NativeSplitter(options_, input) { }

private:
    int32_t pid_selection = 0;
};

class RangePartitionNativeSplitter : public NativeSplitter
{
    void computePartitionId(DB::Block & block) override;
public:
    RangePartitionNativeSplitter(NativeSplitter::Options options_, jobject input);
    ~RangePartitionNativeSplitter() override;
private:
    DB::SortDescription sort_descriptions;
    std::vector<size_t> sorting_key_columns;
    struct SortFieldTypeInfo
    {
        DB::DataTypePtr inner_type;
        bool is_nullable;
    };
    std::vector<SortFieldTypeInfo> sort_field_types;
    DB::Block range_bounds_block;

    std::vector<std::shared_ptr<std::atomic<int>>> part_counts;

    DB::ContextMutablePtr local_context;
    std::atomic<size_t> id_count = 0;
    std::atomic<bool> has_init_dag = false;
    std::mutex init_dag_mutex;
    std::shared_ptr<DB::ExpressionActions> compare_sort_keys_expression;

    std::string getUniqueName();

    static DB::DataTypePtr getCHType(const std::string & spark_type_name);

    void initSortDescriptions(Poco::JSON::Array::Ptr orderings);
    void initSortFieldTypes(Poco::JSON::Array::Ptr orderings);
    void initRangeBlock(Poco::JSON::Array::Ptr range_bounds);

    void computePartitionIdByBinarySearch(DB::Block & block);
    int compareRow(
        const DB::Columns & columns,
        const std::vector<size_t> & required_columns,
        size_t row,
        const DB::Columns & bound_columns,
        size_t bound_row);

    int binarySearchBound(
        const DB::Columns & bound_columns,
        size_t l,
        size_t r,
        const DB::Columns & columns,
        const std::vector<size_t> & used_cols,
        size_t row);

    void initActionsDag(const DB::Block & block);

    void computePartitionIdByActionDag(DB::Block & block);
    DB::ColumnWithTypeAndName buildCompareRightValueColumn(size_t row_pos, size_t col_pos);
    const DB::ActionsDAG::Node * buildColumnCompareExpression(
        std::shared_ptr<DB::ActionsDAG> action_dag,
        int direction,
        size_t left_value_pos,
        DB::ColumnWithTypeAndName right_values,
        std::string & result_name);
    const DB::ActionsDAG::Node * combindTwoCompareExpression(
        std::shared_ptr<DB::ActionsDAG> action_dag,
        const DB::ActionsDAG::Node * left_arg,
        const DB::ActionsDAG::Node * right_arg,
        std::string & result_name);
};

}
