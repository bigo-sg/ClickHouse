#include <Optimizer/OptimizerFactory.h>
#include <Common/Exception.h>
#include <utility>
#include <mutex>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
static std::once_flag init_once_flag;

void registerOrcaOptimizer(OptimizerFactory & instance);
static void registerAllOptimizers(OptimizerFactory & instance)
{
    registerOrcaOptimizer(instance);
}

OptimizerFactory & OptimizerFactory::instance()
{
    static OptimizerFactory instance;
    std::call_once(init_once_flag, [](){ registerAllOptimizers(instance); });
    return instance;
}

CHOptimizerPtr OptimizerFactory::getOptimizer(const String & name)
{
    auto it = optimizers.find(name);
    if (it == optimizers.end())
        return nullptr;
    return it->second();
}

void OptimizerFactory::registerOptimizer(const String & name, OptimizerBuilder optimizer)
{
    auto it = optimizers.find(name);
    if (it != optimizers.end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicated optimizer : {}", name);
    }

    optimizers.emplace(name, optimizer);
}
}
