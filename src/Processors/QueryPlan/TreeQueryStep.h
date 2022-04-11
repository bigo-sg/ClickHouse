#pragma once
#include <memory>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/BlockIOPhaseTransform.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Poco/Logger.h>
namespace DB
{
class TreeQueryStep : public IQueryPlanStep
{
public:
    explicit TreeQueryStep(ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_);
    String getName() const override { return "TreeQueryStep"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
private:
    ContextPtr context;
    QueryBlockIO output_block_io;
    QueryBlockIOs input_block_ios;
    Poco::Logger * logger = &Poco::Logger::get("TreeQueryStep");
};

class ParallelTreeQueryStep : public IQueryPlanStep
{
public:
    explicit ParallelTreeQueryStep(ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_);
    String getName() const override { return "ParallelTreeQueryStep"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
private:
    ContextPtr context;
    QueryBlockIO output_block_io;
    QueryBlockIOs input_block_ios;
    Poco::Logger * logger = &Poco::Logger::get("ParallelTreeQueryStep");
};
}
