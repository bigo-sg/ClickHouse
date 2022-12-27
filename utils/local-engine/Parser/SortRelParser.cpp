#include "SortRelParser.h"
#include <Parser/RelParser.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{

SortRelParser::SortRelParser(SerializedPlanParser * plan_paser_)
    : RelParser(plan_paser_)
{}

DB::QueryPlanPtr
SortRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    size_t limit = parseLimit(rel_stack_);
    const auto & sort_rel = rel.sort();
    auto sort_descr = parseSortDescription(sort_rel.sorts());
    const auto & settings = getContext()->getSettingsRef();
    auto sorting_step = std::make_unique<DB::SortingStep>(
        query_plan->getCurrentDataStream(),
        sort_descr,
        settings.max_block_size,
        limit,
        SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
        settings.max_bytes_before_remerge_sort,
        settings.remerge_sort_lowered_memory_bytes_ratio,
        settings.max_bytes_before_external_sort,
        getContext()->getTemporaryVolume(),
        settings.min_free_disk_space_for_temporary_data);
    sorting_step->setStepDescription("Sorting step");
    query_plan->addStep(std::move(sorting_step));
    return query_plan;
}

DB::SortDescription
SortRelParser::parseSortDescription(const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields, const DB::Block & header)
{
    static std::map<int, std::pair<int, int>> direction_map = {{1, {1, -1}}, {2, {1, 1}}, {3, {-1, 1}}, {4, {-1, -1}}};

    DB::SortDescription sort_descr;
    for (int i = 0, sz = sort_fields.size(); i < sz; ++i)
    {
        const auto & sort_field = sort_fields[i];

        if (!sort_field.expr().has_selection() || !sort_field.expr().selection().has_direct_reference()
            || !sort_field.expr().selection().direct_reference().has_struct_field())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupport sort field");
        }
        auto field_pos = sort_field.expr().selection().direct_reference().struct_field().field();

        auto direction_iter = direction_map.find(sort_field.direction());
        if (direction_iter == direction_map.end())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsuppor sort direction: {}", sort_field.direction());
        }
        if (header.columns())
        {
            auto & col_name = header.getByPosition(field_pos).name;
            sort_descr.emplace_back(col_name, direction_iter->second.first, direction_iter->second.second);
            sort_descr.back().column_number = field_pos;
        }
        else
        {
            sort_descr.emplace_back(field_pos, direction_iter->second.first, direction_iter->second.second);
        }
    }
    return sort_descr;
}

size_t SortRelParser::parseLimit(std::list<const substrait::Rel *> & rel_stack_)
{
    if (rel_stack_.empty())
        return 0;
    const auto & last_rel = *rel_stack_.back();
    if (last_rel.has_fetch())
    {
        const auto & fetch_rel = last_rel.fetch();
        return fetch_rel.count();
    }
    return 0;
}

void registerSortRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser)
    {
        return std::make_shared<SortRelParser>(plan_parser);
    };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kSort, builder);
}
}
