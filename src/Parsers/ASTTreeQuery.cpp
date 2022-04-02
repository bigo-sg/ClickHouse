#include <memory>
#include <IO/Operators.h>
#include <Parsers/ASTTreeQuery.h>
#include <Parsers/formatSettingName.h>
#include "Functions/formatString.h"
#include "Parsers/IAST_fwd.h"
namespace DB
{
ASTPtr ASTTreeQuery::make(ASTPtr output_ast, ASTs input_asts)
{
    auto tree_query = std::make_shared<ASTTreeQuery>();
    tree_query->output_ast = output_ast;
    tree_query->input_asts = input_asts;
    tree_query->children.insert(tree_query->children.end(), input_asts.begin(), input_asts.end());
    tree_query->children.push_back(output_ast);
    return tree_query;
}
ASTPtr ASTTreeQuery::clone() const
{
    auto res = std::make_shared<ASTTreeQuery>();
    for (const auto & ast : input_asts)
    {
        res->input_asts.emplace_back(ast->clone());
        res->children.emplace_back(res->input_asts.back());
    }
    res->output_ast = output_ast->clone();
    res->children.emplace_back(res->output_ast);
    return res;
}

void ASTTreeQuery::formatImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    int i = 0;
    for (const auto & ast : input_asts)
    {
        if (i)
            settings.ostr << "\n";
        ast->format(settings);
        settings.ostr << ";";
        i += 1;
    }
    if (i)
        settings.ostr << "\n";
    output_ast->format(settings);
}

}
