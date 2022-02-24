#include <Parsers/ASTStagedSelectQuery.h>
#include <Parsers/formatSettingName.h>
#include <IO/Operators.h>

namespace DB 
{

void ASTStagedSelectQuery::formatImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
#if 0
    for (const auto & child : children)
    {
        child->format(settings);
        settings.ostr << String(";\n");
    }
#endif
    for (const auto & ast : staged_insert_queries)
    {
        ast->format(settings);
        settings.ostr << ";\n";
    }
    final_select_query->format(settings);
}

ASTPtr ASTStagedSelectQuery::clone() const
{
    auto res = std::make_shared<ASTStagedSelectQuery>();
#if 0    
    for (const auto & child : children)
    {
        res->children.push_back(child->clone());
    }
#endif
    for (const auto & ast : staged_insert_queries)
    {
        res->staged_insert_queries.emplace_back(ast->clone());
    }
    res->final_select_query = final_select_query->clone();
    return res;
}
}
