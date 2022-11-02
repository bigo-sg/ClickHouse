#include "NativeSplitter.h"
#include <functional>
#include <memory>
#include <Functions/FunctionFactory.h>
#include <Parser/SerializedPlanParser.h>
#include <Common/Exception.h>
#include <jni/jni_common.h>
#include <Common/Exception.h>
#include <Common/JNIUtils.h>
#include "Core/Block.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Processors/Transforms/SortingTransform.h"
#include "base/types.h"
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
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ExpressionListParsers.h>
#include <substrait/plan.pb.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>


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
    LOG_ERROR(&Poco::Logger::get("NativeSplitter"), "name:{}, expr:{}", short_name, options_.exprs_buffer);
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
        LOG_ERROR(&Poco::Logger::get("NativeSplitter"), "unsupported splitter. name: {}, expr:{}", short_name, options_.exprs_buffer);
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
    initSortDescriptions(ordering_infos);
    initRangeBlock(info->get("range_bounds").extract<Poco::JSON::Array::Ptr>());
    #if 0
    for (size_t i = 0; i < ordering_infos->size(); ++i)
    {
        SortDescription sorting_descr;
        auto ordering_info = ordering_infos->get(i).extract<Poco::JSON::Object::Ptr>();
        sorting_descr.expression_str = ordering_info->get("expression").convert<std::string>();
        auto type_name = ordering_info->get("data_type").convert<std::string>();
        sorting_descr.data_type = SerializedPlanParser::parseType(type_name);
        sorting_descr.sort_direction = ordering_info->get("direction").convert<int>();
        sorting_descrs.push_back(sorting_descr);
        LOG_ERROR(
            &Poco::Logger::get("RangePartitionNativeSplitter"),
            "XXXX add sort descr: {}; {}; {}",
            sorting_descr.expression_str,
            sorting_descr.data_type->getName(),
            sorting_descr.sort_direction);
    }

    #endif

}

void RangePartitionNativeSplitter::initSortDescriptions(Poco::JSON::Array::Ptr orderings)
{
    for (size_t i = 0; i < orderings->size(); ++i)
    {
        auto ordering = orderings->get(i).extract<Poco::JSON::Object::Ptr>();
        auto expr_str = ordering->get("expression").convert<std::string>();
        substrait::Expression expr;
        google::protobuf::TextFormat::ParseFromString(expr_str, &expr);
        if (!expr.has_selection())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported sorting expression:{}", expr_str);
        }
        auto col_pos = expr.selection().direct_reference().struct_field().field();

        auto sort_direction = ordering->get("direction").convert<int>();
        auto d_iter = direction_map.find(sort_direction);
        if (d_iter == direction_map.end())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported sorting direction:{}", sort_direction);
        }
        DB::SortColumnDescription ch_col_sort_descr(col_pos, d_iter->second.first, d_iter->second.second);
        sort_descriptions.emplace_back(ch_col_sort_descr);

        auto type_name = ordering->get("data_type").convert<std::string>();
        sort_columns_types.emplace_back(SerializedPlanParser::parseType(type_name));
    }
}

void RangePartitionNativeSplitter::initRangeBlock(Poco::JSON::Array::Ptr range_bounds)
{
    DB::ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < sort_columns_types.size(); ++i)
    {
        auto data_type = sort_columns_types[i];
        auto col = data_type->createColumn();
        auto col_name = "sort_col_"  + std::to_string(i);
        for (size_t r = 0; r < range_bounds->size(); ++r)
        {
            auto row = range_bounds->get(r).extract<Poco::JSON::Array::Ptr>();
            if (data_type->getName() == "Int")
            {
                auto val = row->get(i).convert<int>();
                col->insert(val);
            }
            else if (data_type->getName() == "String")
            {
                auto val = row->get(i).convert<std::string>();
                col->insert(val);
            }
            else 
            {
                LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"), "Unsupported data tyep: {}", data_type->getName());
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported data tyep: {}", data_type->getName());    
            }
        }
        columns.emplace_back(std::move(col), data_type, col_name);
    }
    range_block = DB::Block(columns);
}

void RangePartitionNativeSplitter::computePartitionId(DB::Block & block)
{
    LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"), "xxxx input one block({}), rows:{}", block.dumpNames(), block.rows());
    Chunks chunks;
    Chunk chunk(block.getColumns(), block.rows());
    chunks.emplace_back(std::move(chunk));
#if 0
    auto merge_sorter = std::make_unique<DB::MergeSorter>(std::move(chunks), sort_descriptions, 8192, 1000000);
    auto sorted_chunk = merge_sorter->read();
    
    std::vector<size_t> used_cols;
    for (const auto & descr : sort_descriptions)
    {
        used_cols.emplace_back(descr.column_number);
        LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"),  "xxxx add sort col:{}", descr.column_number);
    }
    std::vector<DB::Int64> split_pos;
    size_t iter_row = 0;
    size_t total_rows = sorted_chunk.getNumRows();
    for (size_t i = 0; i < range_block.rows(); ++i)
    {
        DB::Int64 last_pos = -1;
        for (; iter_row < total_rows; ++iter_row)
        {
            auto compare_res = compareRow(sorted_chunk.getColumns(), used_cols, iter_row, range_block.getColumns(), i);
            if (compare_res <= 0)
            {
                last_pos = iter_row;
                continue;
            }
            else
            {
                break;
            }
        }
        split_pos.emplace_back(last_pos);
        LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"), "xxxx add split pos. {} {}", split_pos.size(), last_pos);
    }
    split_pos.emplace_back(total_rows + 1);

    size_t current_id = 0;
    size_t current_split_pos = 0;
    partition_ids.clear();
    for (size_t i = 0; i < total_rows;)
    {
        while(split_pos[current_split_pos] < 0)
        {
            current_split_pos += 1;
        }
        if (i <= static_cast<DB::UInt64>(split_pos[current_split_pos]))
        {
            partition_ids.emplace_back(current_split_pos);
            i += 1;
        }
        else
        {
            current_split_pos += 1;
        }
    }
    LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"),  "xxxx pos ids rows:{}", partition_ids.size());
#else
    std::vector<size_t> used_cols;
    for (const auto & descr : sort_descriptions)
    {
        used_cols.emplace_back(descr.column_number);
        LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"), "xxxx add sort col:{}", descr.column_number);
    }
    partition_ids.clear();
    auto input_columns = block.getColumns();
    auto total_size = block.rows();
    for (size_t r = 0; r < total_size; ++r)
    {
        size_t b = 0;
        for (; b < range_block.rows(); ++b)
        {
            auto res = compareRow(input_columns, used_cols, r, range_block.getColumns(), b);
            if (res <= 0)
            {
                continue;
            }
            else
            {
                break;
            }
        }
        partition_ids.emplace_back(b);
    }

#endif
}

int RangePartitionNativeSplitter::compareRow(const DB::Columns & columns, const std::vector<size_t> & required_columns, size_t row, const DB::Columns & bound_columns, size_t bound_row)
{
    int result = 0;
    for(size_t i = 0, n = required_columns.size(); i < n; ++i)
    {
        auto lpos = required_columns[i];
        auto rpos = i;
        LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"), "xxxx col {}, rows:{}, {}, {}, {}", lpos, columns[lpos]->size(), rpos, row, bound_row);
        auto rcol = bound_columns[rpos]->cloneResized(bound_columns[rpos]->size());
        if (columns[lpos]->isNullable())
        {
            auto n = rcol->size();
            rcol = ColumnNullable::create(std::move(rcol), DB::ColumnUInt8::create(n, 0));
        }
        auto res = columns[lpos]->compareAt(row, bound_row, *rcol, -1);
        if (res != 0)
        {
            result = res;
            break;
        }
    }
    return result;
}

#if 0
void RangePartitionNativeSplitter::computePartitionId(DB::Block & block)
{
    
    NamesWithAliases sorting_columns_names;
    std::set<std::string> distinct_columns;
    DB::WriteBufferFromOwnString write_buf;

    SerializedPlanParser parser(nullptr);
    ActionsDAGPtr expression_dag = std::make_shared<ActionsDAG>(parser.blockToNameAndTypeList(block.cloneEmpty()));
    for (auto & descr : sorting_descrs)
    {
        substrait::Expression expr;
        google::protobuf::TextFormat::ParseFromString(descr.expression_str, &expr);
        LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"), "pb expr:{}", expr.DebugString());
        if (expr.has_selection())
        {
            const auto * field = expression_dag->getInputs()[expr.selection().direct_reference().struct_field().field()];
            if (distinct_columns.count(field->result_name))
            {
                auto unique_name = parser.getUniqueName(field->result_name);
                sorting_columns_names.push_back(NameWithAlias(field->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                sorting_columns_names.push_back(NameWithAlias(field->result_name, field->result_name));
                distinct_columns.emplace(field->result_name);
            }
        }
        else if (expr.has_scalar_function())
        {
            std::string name;
            std::vector<std::string> useless;
            parser.parseFunctionWithDAG(expr, name, useless, expression_dag, true);
            if (!name.empty())
            {
                if (distinct_columns.count(name))
                {
                    auto unique_name = parser.getUniqueName(name);
                    sorting_columns_names.push_back(NameWithAlias(name, unique_name));
                    distinct_columns.emplace(unique_name);
                }
                else
                {
                    sorting_columns_names.push_back(NameWithAlias(name, name));
                    distinct_columns.emplace(name);
                }
            }
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow expression:{}", expr.DebugString());    
        }
    }
    LOG_ERROR(&Poco::Logger::get("RangePartitionNativeSplitter"), "finished action dag");
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Not implemented");
    
}
#endif

}
