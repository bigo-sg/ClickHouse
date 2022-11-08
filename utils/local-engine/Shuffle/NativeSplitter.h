#pragma once
#include <jni.h>
#include <mutex>
#include <stack>
#include <Shuffle/ShuffleSplitter.h>
#include <Common/BlockIterator.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <base/types.h>
#include <Core/SortDescription.h>
#include <DataTypes/Serializations/ISerialization.h>

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
    ~RangePartitionNativeSplitter() override = default;
private:
    DB::SortDescription sort_descriptions;
    std::vector<size_t> sorting_key_columns;
    struct SortFieldTypeInfo
    {
        DB::DataTypePtr inner_type;
        bool is_nullable = false;
    };
    std::vector<SortFieldTypeInfo> sort_field_types;
    DB::Block range_bounds_block;

    std::vector<std::shared_ptr<std::atomic<int>>> part_counts;

    static DB::DataTypePtr getCHType(const std::string & spark_type_name);

    void initSortInformation(Poco::JSON::Array::Ptr orderings);
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
        Int64 l,
        Int64 r,
        const DB::Columns & columns,
        const std::vector<size_t> & used_cols,
        size_t row);
};

}
