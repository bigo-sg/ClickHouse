#pragma once
#include <memory>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
//#include <Processors/Transforms/BlockIOPhaseTransform.h>
#include <Processors/Transforms/StageQueryTransform.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Poco/Logger.h>
namespace DB
{
class StageQueryStep : public IQueryPlanStep
{
public:
    explicit StageQueryStep(ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_);
    String getName() const override { return "StageQueryStep"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
private:
    ContextPtr context;
    QueryBlockIO output_block_io;
    QueryBlockIOs input_block_ios;
    Poco::Logger * logger = &Poco::Logger::get("StageQueryStep");
};

class ParallelStageQueryStep : public IQueryPlanStep
{
public:
    explicit ParallelStageQueryStep(ContextPtr context_, const QueryBlockIO & output_block_io_, const QueryBlockIOs & input_block_ios_);
    String getName() const override { return "ParallelStageQueryStep"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
private:
    ContextPtr context;
    QueryBlockIO output_block_io;
    QueryBlockIOs input_block_ios;
    Poco::Logger * logger = &Poco::Logger::get("ParallelStageQueryStep");
};
}
