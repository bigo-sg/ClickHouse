#include <memory>
#include <IO/Operators.h>
#include <Parsers/ASTStageQuery.h>
#include <Parsers/formatSettingName.h>
#include "Functions/formatString.h"
#include "Parsers/IAST_fwd.h"
namespace DB
{
ASTPtr ASTStageQuery::make(ASTPtr current_query, ASTs upstream_queries)
{
    auto stage_query = std::make_shared<ASTStageQuery>();
    stage_query->current_query = current_query;
    stage_query->upstream_queries = upstream_queries;
    stage_query->children.insert(stage_query->children.end(), upstream_queries.begin(), upstream_queries.end());
    stage_query->children.push_back(current_query);
    return stage_query;
}
ASTPtr ASTStageQuery::clone() const
{
    auto res = std::make_shared<ASTStageQuery>();
    for (const auto & ast : upstream_queries)
    {
        res->upstream_queries.emplace_back(ast->clone());
        res->children.emplace_back(res->upstream_queries.back());
    }
    res->current_query = current_query->clone();
    res->children.emplace_back(res->current_query);
    return res;
}

void ASTStageQuery::formatImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    int i = 0;
    for (const auto & ast : upstream_queries)
    {
        if (i)
            settings.ostr << "\n";
        ast->format(settings);
        settings.ostr << ";";
        i += 1;
    }
    if (i)
        settings.ostr << "\n";
    current_query->format(settings);
}

}
