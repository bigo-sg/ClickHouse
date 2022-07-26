#pragma once

#include <Optimizer/OptimizerFactory.h>
#include "Parsers/IAST_fwd.h"
namespace DB
{
class CHAstPlanResult : public OptimizeResult
{
public:
    /// reuse current interpreters to build a query paln
    ASTPtr rewritten_ast;
};

}
