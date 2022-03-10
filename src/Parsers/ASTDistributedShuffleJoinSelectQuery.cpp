#include <Parsers/ASTDistributedShuffleJoinSelectQuery.h>
#include <Parsers/formatSettingName.h>
#include <IO/Operators.h>

namespace DB 
{

void ASTDistributedShuffleJoinSelectQuery::formatImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
#if 0
    for (const auto & child : children)
    {
        child->format(settings);
        settings.ostr << String(";\n");
    }
#endif
    for (const auto & ast : shuffle_queries)
    {
        ast->format(settings);
        settings.ostr << ";\n";
    }
    select_query->format(settings);
}

ASTPtr ASTDistributedShuffleJoinSelectQuery::clone() const
{
    auto res = std::make_shared<ASTDistributedShuffleJoinSelectQuery>();
#if 0    
    for (const auto & child : children)
    {
        res->children.push_back(child->clone());
    }
#endif
    for (const auto & ast : shuffle_queries)
    {
        res->shuffle_queries.emplace_back(ast->clone());
    }
    res->select_query = select_query->clone();
    return res;
}
}
