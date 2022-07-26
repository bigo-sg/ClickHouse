#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
namespace DB
{

class OptimizerRequest
{
public:
    virtual ~OptimizerRequest() = default;
};
using OptimizerRequestPtr = std::shared_ptr<OptimizerRequest>;

/// A interface class for representing results from a optimizer
class OptimizerResponse
{
public:
    virtual ~OptimizerResponse() = default;
};
using OptimizerResponsePtr = std::shared_ptr<OptimizerResponse>;

/// Optimizer need a session context and a query ast as inputs, and generate a query plan.
/// query plan may be a rewritren ast or a BlockIP that depends on the implemented optimizer.
class ICHOptimizer : public WithContext
{
public:
    virtual ~ICHOptimizer() = default;
    virtual String name() = 0;
    virtual OptimizerResponsePtr optimize(OptimizerRequestPtr request) = 0;
};
using CHOptimizerPtr = std::shared_ptr<ICHOptimizer>;
}
