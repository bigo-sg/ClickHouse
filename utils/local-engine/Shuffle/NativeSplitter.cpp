#include "NativeSplitter.h"
#include <functional>
#include <memory>
#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Common/Exception.h>
#include <boost/asio/detail/eventfd_select_interrupter.hpp>
#include <jni/jni_common.h>
#include <Common/Exception.h>
#include <Common/JNIUtils.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/iostream_debug_helpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Processors/Transforms/SortingTransform.h>
#include <base/types.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Poco/JSON/Array.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Poco/StringTokenizer.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ExpressionListParsers.h>
#include <substrait/plan.pb.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <cstdlib>
#include <mutex>
#include <string>
#include <sys/time.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}
namespace local_engine
{
jclass NativeSplitter::iterator_class = nullptr;
jmethodID NativeSplitter::iterator_has_next = nullptr;
jmethodID NativeSplitter::iterator_next = nullptr;

void NativeSplitter::split(DB::Block & block)
{
    total_rows += block.rows();
    computePartitionId(block);
    DB::IColumn::Selector selector;
    selector = DB::IColumn::Selector(block.rows());
    selector.assign(partition_ids.begin(), partition_ids.end());
    std::vector<DB::Block> partitions;
    for (size_t i = 0; i < options.partition_nums; ++i)
        partitions.emplace_back(block.cloneEmpty());
    for (size_t col = 0; col < block.columns(); ++col)
    {
        DB::MutableColumns scattered = block.getByPosition(col).column->scatter(options.partition_nums, selector);
        for (size_t i = 0; i < options.partition_nums; ++i)
            partitions[i].getByPosition(col).column = std::move(scattered[i]);
    }

    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        auto buffer = partition_buffer[i];
        size_t first_cache_count = std::min(partitions[i].rows(), options.buffer_size - buffer->size());
        if (first_cache_count < partitions[i].rows())
        {
            buffer->add(partitions[i], 0, first_cache_count);
            output_buffer.emplace(std::pair(i, std::make_unique<Block>(buffer->releaseColumns())));
            buffer->add(partitions[i], first_cache_count, partitions[i].rows());
        }
        else
        {
            buffer->add(partitions[i], 0, first_cache_count);
        }
        if (buffer->size() >= options.buffer_size)
        {
            output_buffer.emplace(std::pair(i, std::make_unique<Block>(buffer->releaseColumns())));
        }
    }
}

NativeSplitter::NativeSplitter(Options options_, jobject input_) : options(options_)
{
    GET_JNIENV(env)
    input = env->NewGlobalRef(input_);
    partition_ids.reserve(options.buffer_size);
    partition_buffer.reserve(options.partition_nums);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        partition_buffer.emplace_back(std::make_shared<ColumnsBuffer>());
    }
    CLEAN_JNIENV
}

NativeSplitter::~NativeSplitter()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(input);
    CLEAN_JNIENV
}

bool NativeSplitter::hasNext()
{
    if (begin_ts == 0)
    {
        timeval tv;
        gettimeofday(&tv, nullptr);
        begin_ts = tv.tv_sec * 1000 + tv.tv_usec/1000;
    }
    while (output_buffer.empty())
    {
        if (inputHasNext())
        {
            split(*reinterpret_cast<Block *>(inputNext()));
        }
        else
        {
            for (size_t i = 0; i < options.partition_nums; ++i)
            {
                auto buffer = partition_buffer.at(i);
                if (buffer->size() > 0)
                {
                    output_buffer.emplace(std::pair(i, new Block(buffer->releaseColumns())));
                }
            }
            break;
        }
    }
    if (!output_buffer.empty())
    {
        next_partition_id = output_buffer.top().first;
        setCurrentBlock(*output_buffer.top().second);
        produce();
    }
    if (output_buffer.empty())
    {
        timeval tv;
        gettimeofday(&tv, nullptr);
        DB::UInt64 end_ts = tv.tv_sec * 1000 + tv.tv_usec / 1000;
        LOG_ERROR(&Poco::Logger::get("NativeSplitter"), "splitter metrics. total time:{}, total rows:{}", end_ts - begin_ts, total_rows);
    }
    return !output_buffer.empty();
}

DB::Block * NativeSplitter::next()
{
    if (!output_buffer.empty())
    {
        output_buffer.pop();
    }
    consume();
    return &currentBlock();
}

int32_t NativeSplitter::nextPartitionId()
{
    return next_partition_id;
}

bool NativeSplitter::inputHasNext()
{
    GET_JNIENV(env)
    bool next = safeCallBooleanMethod(env, input, iterator_has_next);
    CLEAN_JNIENV
    return next;
}

int64_t NativeSplitter::inputNext()
{
    GET_JNIENV(env)
    int64_t result = safeCallLongMethod(env, input, iterator_next);
    CLEAN_JNIENV
    return result;
}
std::unique_ptr<NativeSplitter> NativeSplitter::create(const std::string & short_name, Options options_, jobject input)
{
    LOG_ERROR(&Poco::Logger::get("NativeSplitter"), "name:{}, part num:{}, expr:{}", short_name, options_.partition_nums, options_.exprs_buffer);
    if (short_name == "rr")
    {
        return std::make_unique<RoundRobinNativeSplitter>(options_, input);
    }
    else if (short_name == "hash")
    {
        return std::make_unique<HashNativeSplitter>(options_, input);
    }
    else if (short_name == "single")
    {
        options_.partition_nums = 1;
        return std::make_unique<RoundRobinNativeSplitter>(options_, input);
    }
    #if 1
    else if (short_name == "range")
    {
        return std::make_unique<RangePartitionNativeSplitter>(options_, input);
    }
    #endif
    else
    {
        LOG_ERROR(&Poco::Logger::get("NativeSplitter"), "unsupported splitter. name: {}, part num:{}, expr:{}", short_name, options_.partition_nums, options_.exprs_buffer);
        throw std::runtime_error("unsupported splitter " + short_name);
    }
}

HashNativeSplitter::HashNativeSplitter(NativeSplitter::Options options_, jobject input)
    : NativeSplitter(options_, input)
{
    Poco::StringTokenizer exprs_list(options_.exprs_buffer, ",");
    hash_fields.insert(hash_fields.end(), exprs_list.begin(), exprs_list.end());
}

void HashNativeSplitter::computePartitionId(Block & block)
{
    ColumnsWithTypeAndName args;
    for (auto & name : hash_fields)
    {
        args.emplace_back(block.getByName(name));
    }
    if (!hash_function)
    {
        auto & factory = DB::FunctionFactory::instance();
        auto function = factory.get("murmurHash3_32", local_engine::SerializedPlanParser::global_context);

        hash_function = function->build(args);
    }
    auto result_type = hash_function->getResultType();
    auto hash_column = hash_function->execute(args, result_type, block.rows(), false);
    partition_ids.clear();
    for (size_t i = 0; i < block.rows(); i++)
    {
        partition_ids.emplace_back(static_cast<UInt64>(hash_column->get64(i) % options.partition_nums));
    }
}

void RoundRobinNativeSplitter::computePartitionId(Block & block)
{
    partition_ids.resize(block.rows());
    for (auto & pid : partition_ids)
    {
        pid = pid_selection;
        pid_selection = (pid_selection + 1) % options.partition_nums;
    }
}

static std::map<int, std::pair<int, int>> direction_map = {
        {1, {1, -1}},
        {2, {1, 1}},
        {3, {-1, 1}},
        {4, {-1, -1}}
};

RangePartitionNativeSplitter::RangePartitionNativeSplitter(NativeSplitter::Options options_, jobject input)
    : NativeSplitter(options_, input)
{
    Poco::JSON::Parser parser;
    auto info = parser.parse(options_.exprs_buffer).extract<Poco::JSON::Object::Ptr>();

    auto ordering_infos = info->get("ordering").extract<Poco::JSON::Array::Ptr>();
    initSortInformation(ordering_infos);
    initRangeBlock(info->get("range_bounds").extract<Poco::JSON::Array::Ptr>());
}

void RangePartitionNativeSplitter::initSortInformation(Poco::JSON::Array::Ptr orderings)
{
    for (size_t i = 0; i < orderings->size(); ++i)
    {
        auto ordering = orderings->get(i).extract<Poco::JSON::Object::Ptr>();
        auto col_pos = ordering->get("column_ref").convert<DB::Int32>();

        auto sort_direction = ordering->get("direction").convert<int>();
        auto d_iter = direction_map.find(sort_direction);
        if (d_iter == direction_map.end())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported sorting direction:{}", sort_direction);
        }
        DB::SortColumnDescription ch_col_sort_descr(col_pos, d_iter->second.first, d_iter->second.second);
        sort_descriptions.emplace_back(ch_col_sort_descr);
        
        auto type_name = ordering->get("data_type").convert<std::string>();
        auto type = SerializedPlanParser::parseType(type_name);
        SortFieldTypeInfo info;
        info.inner_type = type;
        info.is_nullable = ordering->get("is_nullable").convert<bool>();
        sort_field_types.emplace_back(info);
        sorting_key_columns.emplace_back(col_pos);
    }
}

void RangePartitionNativeSplitter::initRangeBlock(Poco::JSON::Array::Ptr range_bounds)
{
    DB::ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < sort_field_types.size(); ++i)
    {
        auto & type_info = sort_field_types[i];
        auto inner_col = type_info.inner_type->createColumn();
        DB::MutableColumnPtr col = std::move(inner_col);
        if (type_info.is_nullable)
            col = ColumnNullable::create(std::move(col), DB::ColumnUInt8::create(0, 0));
        for (size_t r = 0; r < range_bounds->size(); ++r)
        {
            auto row = range_bounds->get(r).extract<Poco::JSON::Array::Ptr>();
            auto field_info = row->get(i).extract<Poco::JSON::Object::Ptr>();
            if (field_info->get("is_null").convert<bool>())
            {
                col->insertData(nullptr, 0);
            }
            else
            {
                const auto & type_name = type_info.inner_type->getName();
                const auto & field_value = field_info->get("value");
                if (type_name == "UInt8")
                {
                    col->insert(static_cast<DB::UInt8>(field_value.convert<DB::Int16>()));
                }
                else if (type_name == "Int8")
                {
                    col->insert(field_value.convert<DB::Int8>());
                }
                else if (type_name == "Int16")
                {
                    col->insert(field_value.convert<DB::Int16>());
                }
                else if (type_name == "Int32")
                {
                    col->insert(field_value.convert<DB::Int32>());
                }
                else if(type_name == "Int64")
                {
                    col->insert(field_value.convert<DB::Int64>());
                }
                else if (type_name == "Float32")
                {
                    col->insert(field_value.convert<DB::Float32>());
                }
                else if (type_name == "Float64")
                {
                    col->insert(field_value.convert<DB::Float64>());
                }
                else if (type_name == "String")
                {
                    col->insert(field_value.convert<std::string>());
                }
                else if (type_name == "Date")
                {
                    int val = field_value.convert<DB::UInt16>();
                    col->insert(val);
                }
                else
                {
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported data type: {}", type_info.inner_type->getName());
                }
            }
        }
        auto data_type = type_info.inner_type;
        if (type_info.is_nullable)
        {
            data_type = std::make_shared<DB::DataTypeNullable>(data_type);
        }
        auto col_name = "sort_col_"  + std::to_string(i);
        columns.emplace_back(std::move(col), data_type, col_name);
    }
    range_bounds_block = DB::Block(columns);
}
void RangePartitionNativeSplitter::computePartitionId(DB::Block & block)
{
    computePartitionIdByBinarySearch(block);
}

void RangePartitionNativeSplitter::computePartitionIdByBinarySearch(DB::Block & block)
{
    Chunks chunks;
    Chunk chunk(block.getColumns(), block.rows());
    chunks.emplace_back(std::move(chunk));
    partition_ids.clear();
    partition_ids.reserve(block.rows());
    auto input_columns = block.getColumns();
    auto total_rows = block.rows();
    const auto & bounds_columns = range_bounds_block.getColumns();
    auto max_part = bounds_columns[0]->size();
    for (size_t r = 0; r < total_rows; ++r)
    {
        size_t selected_partition = 0;
        auto ret = binarySearchBound(bounds_columns, 0, max_part - 1, input_columns, sorting_key_columns, r);
        if (ret >= 0)
            selected_partition = ret;
        else
            selected_partition = max_part;
        partition_ids.emplace_back(selected_partition);
    }

}

int RangePartitionNativeSplitter::compareRow(
    const DB::Columns & columns,
    const std::vector<size_t> & required_columns,
    size_t row,
    const DB::Columns & bound_columns,
    size_t bound_row)
{
    int result = 0;
    for(size_t i = 0, n = required_columns.size(); i < n; ++i)
    {
        auto lpos = required_columns[i];
        auto rpos = i;
        auto res = columns[lpos]->compareAt(row, bound_row, *bound_columns[rpos], sort_descriptions[i].nulls_direction)
            * sort_descriptions[i].direction;
        if (res != 0)
        {
            result = res;
            break;
        }
    }
    return result;
}

// If there were elements in range[l,r] that are larger then the row
// the return the min element's index. otherwise return -1
int RangePartitionNativeSplitter::binarySearchBound(
    const DB::Columns & bound_columns,
    Int64 l,
    Int64 r,
    const DB::Columns & columns,
    const std::vector<size_t> & used_cols,
    size_t row)
{
    if (l > r)
    {
        return -1;
    }
    auto m = (l + r) >> 1;
    auto cmp_ret = compareRow(columns, used_cols, row, bound_columns, m);
    if (l == r)
    {
        if (cmp_ret <= 0)
            return static_cast<int>(m);
        else
            return -1;
    }
    
    if (cmp_ret == 0)
        return static_cast<int>(m);
    if (cmp_ret < 0)
    {
        cmp_ret = binarySearchBound(bound_columns, l, m - 1, columns, used_cols, row);
        if (cmp_ret < 0)
        {
            // m is the upper bound
            return static_cast<int>(m);
        }
        return cmp_ret;

    }
    else
    {
        cmp_ret = binarySearchBound(bound_columns, m + 1, r, columns, used_cols, row);
        if (cmp_ret < 0)
            return -1;
        else
            return cmp_ret;
    }
    __builtin_unreachable();
}


}
