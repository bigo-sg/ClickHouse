#include <Functions/FunctionFactory.h>

namespace local_engine
{

using namespace DB;
void registerFunctionSparkTrim(FunctionFactory &);
void registerFunctionsHashingExtended(FunctionFactory & factory);

void registerFunctions(FunctionFactory  & factory)
{
    registerFunctionSparkTrim(factory);
    registerFunctionsHashingExtended(factory);
}
}
