#include "AggregatingStepBaseCardinality.h"
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/MultiPathSelectStep.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/HighCardinalityAggregatingTransform.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
AggregatingAlgorithmSelector::AggregatingAlgorithmSelector(
    std::shared_ptr<BlockStatMetadata> stat_info_,
    Aggregator::Params params_,
    UInt32 high_card_threshold_)
    : stat_info(stat_info_)
    , params(params_)
    , high_card_threshold(high_card_threshold_)
{
}

Int32 AggregatingAlgorithmSelector::getPath()
{
    // CardinalityType::LOW is the default algorithm.

    if (num_streams < 2)
        return CardinalityType::LOW;


    std::map<String, size_t> name_indexes;
    size_t i = 0;
    for (const auto & col : stat_info->columns_metadata)
    {
        name_indexes[col->column_name] = i;
        i += 1;
    }

    bool has_high_card = false;
    for (const auto & key : params.keys)
    {
        auto iter = name_indexes.find(key);
        if (iter == name_indexes.end())
            continue;
        const auto & col_stat = stat_info->columns_metadata[iter->second];
        LOG_ERROR(
            &Poco::Logger::get("AggregatingAlgorithmSelector"),
            "col:{}, distinct cnt:{}, rows:{}, threshold:{}",
            key,
            col_stat->distinct_count,
            col_stat->rows,
            high_card_threshold);
        if (col_stat->distinct_count * 100 / (col_stat->rows + 1) > high_card_threshold)
        {
            has_high_card = true;
            break;
        }
    }

    if (has_high_card)
        return CardinalityType::HIGH;
    return CardinalityType::LOW;
}

LowCardinalityAggregatingExecution::LowCardinalityAggregatingExecution(
    Aggregator::Params params_,
    size_t merge_threads_,
    bool final_,
    size_t temporary_data_merge_threads_)
    : params(params_)
    , merge_threads(merge_threads_)
    , final(final_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
}

DynamicPathBuilder::PathResult LowCardinalityAggregatingExecution::buildPath(size_t num_streams, const OutputPortRawPtrs & outports)
{
    PathResult result;
    bool allow_to_use_two_level_group_by = num_streams > 1 || params.max_bytes_before_external_group_by != 0;

    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }
    
    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    const auto & src_header = outports[0]->getHeader();
    auto transform_params = std::make_shared<AggregatingTransformParams>(src_header, params, final);
    OutputPortRawPtrs current_outports = outports;
    if (num_streams > 1)
    {
        auto many_data = std::make_shared<ManyAggregatedData>(num_streams);
        size_t counter = 0;
        auto [agg_processors, agg_outports] = QueryPipelineBuilder::connectProcessors(
            [&](const std::vector<Block> & headers)
            {
                return std::make_shared<AggregatingTransform>(
                    headers[0], transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
            },
            current_outports);
        result.processors.insert(result.processors.end(), agg_processors.begin(), agg_processors.end());
        current_outports.swap(agg_outports);
    }
    else
    {
        auto [agg_processors, agg_outports] = QueryPipelineBuilder::connectProcessors(
            [&](const std::vector<Block> & headers) { return std::make_shared<AggregatingTransform>(headers[0], transform_params); },
            current_outports);
        result.processors.insert(result.processors.end(), agg_processors.begin(), agg_processors.end());
        current_outports.swap(agg_outports);
    }
    result.output_ports.swap(current_outports);
    return result;    
}

HighCardinalityAggregatingExecution::HighCardinalityAggregatingExecution(Aggregator::Params params_, bool final_)
    : params(params_), final(final_)
{}

DynamicPathBuilder::PathResult HighCardinalityAggregatingExecution::buildPath(size_t num_streams, const OutputPortRawPtrs & outports)
{
    #if 0
    PathResult result;
    OutputPortRawPtrs current_output_ports = outports;
    auto src_header = outports[0]->getHeader();
    std::vector<size_t> keys;
    for (const auto & key_name : params.keys)
    {
        keys.push_back(src_header.getPositionByName(key_name));
    }
    auto [shuffle_processors, shuffle_output_ports] = QueryPipelineBuilder::connectProcessors(
        [&](const std::vector<Block> & headers) { return std::make_shared<InnerShuffleTransform>(num_streams, headers[0], keys); },
        current_output_ports);
    result.processors.insert(result.processors.end(), shuffle_processors.begin(), shuffle_processors.end());
    current_output_ports.swap(shuffle_output_ports);
    assert(current_output_ports.size() == num_streams * num_streams);

    OutputPortRawPtrs union_output_ports;
    for (size_t i = 0; i < num_streams; ++i)
    {
        OutputPortRawPtrs ports_for_input;
        for (size_t j = 0; j < num_streams; ++j)
        {
            ports_for_input.push_back(current_output_ports[j * num_streams + i]);
        }
        auto [union_processors, ports] = QueryPipelineBuilder::connectProcessors(
            [&](const std::vector<Block> & headers) { return std::make_shared<UnionStreamsTransform>(headers[0], num_streams); },
            ports_for_input, num_streams);
        result.processors.insert(result.processors.end(), union_processors.begin(), union_processors.end());
        union_output_ports.insert(union_output_ports.end(), ports.begin(), ports.end());
    }
    current_output_ports.swap(union_output_ports);
    assert(current_output_ports.size() == num_streams);

    auto [agg_processors, agg_output_ports] = QueryPipelineBuilder::connectProcessors(
        [&](const std::vector<Block> & headers)
        { return std::make_shared<HighCardinalityAggregatingTransform>(0, 1, headers[0], this->params, this->final); },
        current_output_ports);
    result.processors.insert(result.processors.end(), agg_processors.begin(), agg_processors.end());
    current_output_ports.swap(agg_output_ports);
    result.output_ports.swap(current_output_ports);

    return result;

    #else
    PathResult result;
    OutputPortRawPtrs current_output_ports = outports;
    auto src_header = outports[0]->getHeader();
    std::vector<size_t> keys;
    for (const auto & key_name : params.keys)
    {
        keys.push_back(src_header.getPositionByName(key_name));
    }

    auto [add_hash_processors, add_hash_outputs] = QueryPipelineBuilder::connectProcessors(
        [&](const std::vector<Block> & headers) { return std::make_shared<BuildAggregatingKeysHashColumnTransform>(headers[0], keys, num_streams); },
        current_output_ports);
    current_output_ports.swap(add_hash_outputs);
    result.processors.insert(result.processors.end(), add_hash_processors.begin(), add_hash_processors.end());
    LOG_ERROR(
        &Poco::Logger::get("HighCardinalityAggregatingExecution"),
        "xxxx {} out header={}",
        __LINE__,
        current_output_ports[0]->getHeader().dumpNames());

    auto [copy_processors, copy_outputs] = QueryPipelineBuilder::connectProcessors(
        [&](const std::vector<Block> & headers) { return std::make_shared<CopyTransform>(headers[0], num_streams); },
        current_output_ports);
    current_output_ports.swap(copy_outputs);
    result.processors.insert(result.processors.end(), copy_processors.begin(), copy_processors.end());
    assert(current_output_ports.size() == num_streams * num_streams);
    LOG_ERROR(&Poco::Logger::get("HighCardinalityAggregatingExecution"), "xxxx {} current_output_ports={}", __LINE__, current_output_ports.size());
    LOG_ERROR(
        &Poco::Logger::get("HighCardinalityAggregatingExecution"),
        "xxxx {} out header={}",
        __LINE__,
        current_output_ports[0]->getHeader().dumpNames());

    OutputPortRawPtrs resize_outputs;
    for (size_t i = 0; i < num_streams; ++i)
    {
        OutputPortRawPtrs input_ports;
        for (size_t j = 0; j < num_streams; ++j)
        {
            input_ports.emplace_back(current_output_ports[j * num_streams + i]);
        }
        auto [resize_processors, part_resize_outputs] = QueryPipelineBuilder::connectProcessors(
            [&](const std::vector<Block> & headers) { return std::make_shared<ResizeProcessor>(headers[0], num_streams, 1); },
            input_ports,
            num_streams);
        resize_outputs.insert(resize_outputs.end(), part_resize_outputs.begin(), part_resize_outputs.end());
        result.processors.insert(result.processors.end(), resize_processors.begin(), resize_processors.end());
    }
    current_output_ports.swap(resize_outputs);
    assert(current_output_ports.size() == num_streams);
    LOG_ERROR(&Poco::Logger::get("HighCardinalityAggregatingExecution"), "xxxx {} current_output_ports={}", __LINE__, current_output_ports.size());
    LOG_ERROR(
        &Poco::Logger::get("HighCardinalityAggregatingExecution"),
        "xxxx {} out header={}",
        __LINE__,
        current_output_ports[0]->getHeader().dumpNames());

    size_t id = 0;
    auto [agg_processors, agg_outputs] = QueryPipelineBuilder::connectProcessors(
        [&](const std::vector<Block> & headers)
        { return std::make_shared<HighCardinalityAggregatingTransform>(id++, num_streams, headers[0], this->params, this->final); },
        current_output_ports);
    current_output_ports.swap(agg_outputs);
    LOG_ERROR(
        &Poco::Logger::get("HighCardinalityAggregatingExecution"),
        "xxxx {} out header={}",
        __LINE__,
        current_output_ports[0]->getHeader().dumpNames());
    result.processors.insert(result.processors.end(), agg_processors.begin(), agg_processors.end());
    result.output_ports.swap(current_output_ports);
    LOG_ERROR(&Poco::Logger::get("HighCardinalityAggregatingExecution"), "xxxx {} current_output_ports={}", __LINE__, current_output_ports.size());

    return result;
    #endif
}

}
