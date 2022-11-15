#include "SelectorBuilder.h"
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Parser/SerializedPlanParser.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}
namespace local_engine
{
std::vector<DB::IColumn::ColumnIndex> RoundRobinSelectorBuilder::build(DB::Block & block)
{
    std::vector<DB::IColumn::ColumnIndex> result;
    result.resize(block.rows());
    for (auto & pid : result)
    {
        pid = pid_selection;
        pid_selection = (pid_selection + 1) % parts_num;
    }
    return result;
}

HashSelectorBuilder::HashSelectorBuilder(UInt32 parts_num_, const std::vector<std::string> & exprs_, const std::string & hash_function_name_)
    : parts_num(parts_num_), exprs(exprs_), hash_function_name(hash_function_name_)
{
}

std::vector<DB::IColumn::ColumnIndex> HashSelectorBuilder::build(DB::Block & block)
{
    ColumnsWithTypeAndName args;
    for (auto & name : exprs)
    {
        args.emplace_back(block.getByName(name));
    }

    if (!hash_function) [[unlikely]]
    {
        auto & factory = DB::FunctionFactory::instance();
        auto function = factory.get(hash_function_name, local_engine::SerializedPlanParser::global_context);

        hash_function = function->build(args);
    }
    std::vector<DB::IColumn::ColumnIndex> partition_ids;
    auto result_type = hash_function->getResultType();
    auto hash_column = hash_function->execute(args, result_type, block.rows(), false);

    for (size_t i = 0; i < block.rows(); i++)
    {
        partition_ids.emplace_back(static_cast<UInt64>(hash_column->get64(i) % parts_num));
    }
    return partition_ids;
}


static std::map<int, std::pair<int, int>> direction_map = {
        {1, {1, -1}},
        {2, {1, 1}},
        {3, {-1, 1}},
        {4, {-1, -1}}
};

RangeSelectorBuilder::RangeSelectorBuilder(const std::string & option)
{
    Poco::JSON::Parser parser;
    auto info = parser.parse(option).extract<Poco::JSON::Object::Ptr>();
    auto ordering_infos = info->get("ordering").extract<Poco::JSON::Array::Ptr>();
    initSortInformation(ordering_infos);
    initRangeBlock(info->get("range_bounds").extract<Poco::JSON::Array::Ptr>());
}

std::vector<DB::IColumn::ColumnIndex> RangeSelectorBuilder::build(DB::Block & block)
{
    std::vector<DB::IColumn::ColumnIndex> result;
    computePartitionIdByBinarySearch(block, result);
    return result;
}

void RangeSelectorBuilder::initSortInformation(Poco::JSON::Array::Ptr orderings)
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

void RangeSelectorBuilder::initRangeBlock(Poco::JSON::Array::Ptr range_bounds)
{
    DB::ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < sort_field_types.size(); ++i)
    {
        auto & type_info = sort_field_types[i];
        auto inner_col = type_info.inner_type->createColumn();
        auto data_type = type_info.inner_type;
        DB::MutableColumnPtr col = std::move(inner_col);
        if (type_info.is_nullable)
        {
            col = ColumnNullable::create(std::move(col), DB::ColumnUInt8::create(0, 0));
            data_type = std::make_shared<DB::DataTypeNullable>(data_type);
        }
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
        auto col_name = "sort_col_"  + std::to_string(i);
        columns.emplace_back(std::move(col), data_type, col_name);
    }
    range_bounds_block = DB::Block(columns);
}

void RangeSelectorBuilder::computePartitionIdByBinarySearch(DB::Block & block, std::vector<DB::IColumn::ColumnIndex> & selector)
{
    Chunks chunks;
    Chunk chunk(block.getColumns(), block.rows());
    chunks.emplace_back(std::move(chunk));
    selector.clear();
    selector.reserve(block.rows());
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
        selector.emplace_back(selected_partition);
    }
}

int RangeSelectorBuilder::compareRow(
    const DB::Columns & columns,
    const std::vector<size_t> & required_columns,
    size_t row,
    const DB::Columns & bound_columns,
    size_t bound_row)
{
    for(size_t i = 0, n = required_columns.size(); i < n; ++i)
    {
        auto lpos = required_columns[i];
        auto rpos = i;
        auto res = columns[lpos]->compareAt(row, bound_row, *bound_columns[rpos], sort_descriptions[i].nulls_direction)
            * sort_descriptions[i].direction;
        if (res != 0)
        {
            return res;
        }
    }
    return 0;
}

// If there were elements in range[l,r] that are larger then the row
// the return the min element's index. otherwise return -1
int RangeSelectorBuilder::binarySearchBound(
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
