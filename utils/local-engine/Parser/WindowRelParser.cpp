#include "WindowRelParser.h"
#include <exception>
#include <memory>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/SortDescription.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/WindowDescription.h>
#include <Parser/RelParser.h>
#include <Parser/SortRelParser.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <base/logger_useful.h>
#include <base/sort.h>
#include <google/protobuf/util/json_util.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}
}
namespace local_engine
{

WindowRelParser::WindowRelParser(SerializedPlanParser * plan_paser_) : RelParser(plan_paser_)
{
}

DB::QueryPlanPtr
WindowRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    // rel_stack = rel_stack_;
    current_plan = std::move(current_plan_);
    const auto & win_rel_pb = rel.window();
    auto window_descriptions = parseWindowDescriptions(win_rel_pb);

    auto expected_header = current_plan->getCurrentDataStream().header;
    for (const auto & measure : win_rel_pb.measures())
    {
        const auto & win_function = measure.measure();
        ColumnWithTypeAndName named_col;
        named_col.name = win_function.column_name();
        named_col.type = parseType(win_function.output_type());
        named_col.column = named_col.type->createColumn();
        expected_header.insert(named_col);
    }
    /// In spark plan, there is already a sort step before each window, so we don't need to add sort steps here.
    for (auto & it : window_descriptions)
    {
        auto & win = it.second;
        ;
        auto window_step = std::make_unique<DB::WindowStep>(current_plan->getCurrentDataStream(), win, win.window_functions);
        window_step->setStepDescription("Window step for window '" + win.window_name + "'");
        current_plan->addStep(std::move(window_step));
    }
    auto current_header = current_plan->getCurrentDataStream().header;
    if (!DB::blocksHaveEqualStructure(expected_header, current_header))
    {
        ActionsDAGPtr convert_action = ActionsDAG::makeConvertingActions(
            current_header.getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            DB::ActionsDAG::MatchColumnsMode::Name);
        QueryPlanStepPtr convert_step = std::make_unique<DB::ExpressionStep>(current_plan->getCurrentDataStream(), convert_action);
        convert_step->setStepDescription("Convert window Output");
        current_plan->addStep(std::move(convert_step));
    }

    DB::WriteBufferFromOwnString ss;
    current_plan->explainPlan(ss, DB::QueryPlan::ExplainPlanOptions{});
    return std::move(current_plan);
}

std::unordered_map<DB::String, WindowDescription> WindowRelParser::parseWindowDescriptions(const substrait::WindowRel & win_rel)
{
    std::unordered_map<DB::String, WindowDescription> window_descriptions;

    for (int i = 0; i < win_rel.measures_size(); ++i)
    {
        const auto & measure = win_rel.measures(i);
        const auto win_func_pb = measure.measure();
        auto window_name = getWindowName(win_rel, win_func_pb);
        auto win_it = window_descriptions.find(window_name);
        WindowDescription * description = nullptr;
        if (win_it == window_descriptions.end())
        {
            WindowDescription new_result;
            window_descriptions[window_name] = new_result;
            description = &window_descriptions[window_name];
            description->window_name = window_name;
            description->frame = parseWindowFrame(win_func_pb);
            description->partition_by = parsePartitionBy(win_rel.partition_expressions());
            description->order_by = SortRelParser::parseSortDescription(win_rel.sorts(), current_plan->getCurrentDataStream().header);
            description->full_sort_description = description->partition_by;
            description->full_sort_description.insert(
                description->full_sort_description.end(), description->order_by.begin(), description->order_by.end());
        }
        else
        {
            description = &win_it->second;
        }

        auto win_func = parseWindowFunctionDescription(win_rel, win_func_pb);
        description->window_functions.emplace_back(win_func);
    }
    return window_descriptions;
}

DB::WindowFrame WindowRelParser::parseWindowFrame(const substrait::Expression::WindowFunction & window_function)
{
    DB::WindowFrame win_frame;
    win_frame.type = parseWindowFrameType(window_function);
    parseBoundType(window_function.lower_bound(), true, win_frame.begin_type, win_frame.begin_offset, win_frame.begin_preceding);
    parseBoundType(window_function.upper_bound(), false, win_frame.end_type, win_frame.end_offset, win_frame.end_preceding);
    return win_frame;
}

DB::WindowFrame::FrameType WindowRelParser::parseWindowFrameType(const substrait::Expression::WindowFunction & window_function)
{
    const auto & win_type = window_function.window_type();
    // It's weird! The frame type only could be rows in spark for rank(). But in clickhouse
    // it's should be range. If run rank() over rows frame, the result is different. The rank number
    // is different for the same values.
    auto function_name = parseFunctionName(window_function.function_reference());
    if (function_name && *function_name == "rank")
    {
        return DB::WindowFrame::FrameType::Range;
    }
    if (win_type == substrait::ROWS)
    {
        return DB::WindowFrame::FrameType::Rows;
    }
    else if (win_type == substrait::RANGE)
    {
        return DB::WindowFrame::FrameType::Range;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unknow window frame type:{}", win_type);
    }
}

void WindowRelParser::parseBoundType(
    const substrait::Expression::WindowFunction::Bound & bound,
    bool is_begin_or_end,
    DB::WindowFrame::BoundaryType & bound_type,
    Field & offset,
    bool & preceding_direction)
{
    /// some default settings.
    offset = 0;

    if (bound.has_preceding())
    {
        const auto & preceding = bound.preceding();
        bound_type = DB::WindowFrame::BoundaryType::Offset;
        preceding_direction = preceding.offset() >= 0;
        if (preceding.offset() < 0)
        {
            offset = 0 - preceding.offset();
        }
        else
        {
            offset = preceding.offset();
        }
    }
    else if (bound.has_following())
    {
        const auto & following = bound.following();
        bound_type = DB::WindowFrame::BoundaryType::Offset;
        preceding_direction = following.offset() < 0;
        if (following.offset() < 0)
        {
            offset = 0 - following.offset();
        }
        else
        {
            offset = following.offset();
        }
    }
    else if (bound.has_current_row())
    {
        const auto & current_row = bound.current_row();
        bound_type = DB::WindowFrame::BoundaryType::Current;
        offset = 0;
        preceding_direction = is_begin_or_end;
    }
    else if (bound.has_unbounded_preceding())
    {
        bound_type = DB::WindowFrame::BoundaryType::Unbounded;
        offset = 0;
        preceding_direction = true;
    }
    else if (bound.has_unbounded_following())
    {
        bound_type = DB::WindowFrame::BoundaryType::Unbounded;
        offset = 0;
        preceding_direction = false;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unknown bound type:{}", bound.DebugString());
    }
}


DB::SortDescription WindowRelParser::parsePartitionBy(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions)
{
    DB::Block header = current_plan->getCurrentDataStream().header;
    DB::SortDescription sort_descr;
    for (const auto & expr : expressions)
    {
        if (!expr.has_selection())
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Column reference is expected.");
        }
        auto pos = expr.selection().direct_reference().struct_field().field();
        auto col_name = header.getByPosition(pos).name;
        sort_descr.push_back(DB::SortColumnDescription(col_name, 1, 1));
    }
    return sort_descr;
}

WindowFunctionDescription WindowRelParser::parseWindowFunctionDescription(
    const substrait::WindowRel & win_rel, const substrait::Expression::WindowFunction & window_function)
{
    auto header = current_plan->getCurrentDataStream().header;
    WindowFunctionDescription description;
    description.column_name = window_function.column_name();
    description.function_node = nullptr;

    auto function_name = parseFunctionName(window_function.function_reference());
    if (!function_name)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found function for reference: {}", window_function.function_reference());
    DB::AggregateFunctionProperties agg_function_props;
    auto arg_types = parseFunctionArgumentTypes(header, window_function.arguments());
    auto arg_names = parseFunctionArgumentNames(header, window_function.arguments());
    auto agg_function_ptr = getAggregateFunction(*function_name, arg_types, agg_function_props);

    description.argument_names = arg_names;
    description.argument_types = arg_types;
    description.aggregate_function = agg_function_ptr;

    return description;
}

String WindowRelParser::getWindowName(const substrait::WindowRel & win_rel, const substrait::Expression::WindowFunction & window_function)
{
    DB::WriteBufferFromOwnString ss;
    ss << getWindowFunctionColumnName(win_rel);
    google::protobuf::util::JsonPrintOptions printOption;
    printOption.always_print_primitive_fields = true;
    printOption.add_whitespace = false;
    String frame_type_str;
    auto frame_type = parseWindowFrameType(window_function);
    switch (frame_type)
    {
        case DB::WindowFrame::FrameType::Rows:
            frame_type_str = "Rows";
            break;
        case DB::WindowFrame::FrameType::Range:
            frame_type_str = "Range";
            break;
        default:
            break;
    }
    ss << " " << frame_type_str;
    String upper_bound_str;
    google::protobuf::util::MessageToJsonString(window_function.upper_bound(), &upper_bound_str, printOption);
    String lower_bound_str;
    google::protobuf::util::MessageToJsonString(window_function.lower_bound(), &lower_bound_str, printOption);
    ss << " BETWEEN " << lower_bound_str << " AND " << upper_bound_str;
    return ss.str();
}
String WindowRelParser::getWindowFunctionColumnName(const substrait::WindowRel & win_rel)
{
    google::protobuf::util::JsonPrintOptions printOption;
    printOption.always_print_primitive_fields = true;
    printOption.add_whitespace = false;
    DB::WriteBufferFromOwnString ss;
    size_t n = 0;
    ss << "PATITION BY ";
    for (const auto & expr : win_rel.partition_expressions())
    {
        if (n)
            ss << ",";
        String partition_exprs_str;
        google::protobuf::util::MessageToJsonString(expr, &partition_exprs_str, printOption);
        ss << partition_exprs_str;
        n++;
    }
    ss << " ORDER BY ";
    n = 0;
    for (const auto & field : win_rel.sorts())
    {
        if (n)
            ss << ",";
        String order_by_str;
        google::protobuf::util::MessageToJsonString(field, &order_by_str, printOption);
        ss << order_by_str;
    }
    return ss.str();
}

void registerWindowRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_paser) { return std::make_shared<WindowRelParser>(plan_paser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kWindow, builder);
}
}
