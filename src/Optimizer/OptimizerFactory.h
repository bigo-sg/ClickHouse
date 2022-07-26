#pragma once
#include <boost/core/noncopyable.hpp>
#include <Optimizer/ICHOptimizer.h>
#include <map>

namespace DB
{

class OptimizerFactory : public boost::noncopyable
{
public:
    using OptimizerBuilder = std::function<CHOptimizerPtr()>;
    static OptimizerFactory & instance();
    CHOptimizerPtr getOptimizer(const String & name);

    void registerOptimizer(const String & name, OptimizerBuilder optimizer);
protected:
    OptimizerFactory() = default;

private:
    std::map<String, OptimizerBuilder> optimizers;
};
}
