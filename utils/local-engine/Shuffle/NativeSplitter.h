#pragma once
#include <jni.h>
#include <stack>
#include <Shuffle/ShuffleSplitter.h>
#include <Common/BlockIterator.h>
#include "DataTypes/Serializations/ISerialization.h"

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
private:
    struct SortDescription
    {
        std::string expression_str;
        DB::DataTypePtr data_type;
        int sort_direction;
    };
    std::vector<SortDescription> sorting_descrs;

    static DB::DataTypePtr getCHType(const std::string & spark_type_name);
};

}
