#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <vector>

namespace DB
{
class InnerShuffleStep : public IQueryPlanStep
{
public:
    InnerShuffleStep(const DataStream & stream_, const std::vector<size_t> & hash_columns_);
    String getName() const override { return "InnerShuffleStep"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;
    void describePipeline(FormatSettings & settings) const override;
private:
    std::vector<size_t> hash_columns;
    Processors processors;
};
}
