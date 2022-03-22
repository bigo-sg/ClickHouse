#include <memory>
#include <Parsers/ASTShufflePhasesSelectQuery.h>
#include <Parsers/formatSettingName.h>
#include <IO/Operators.h>

namespace DB
{
ASTPtr ASTShufflePhasesSelectQuery::clone() const
{
    auto res = std::make_shared<ASTShufflePhasesSelectQuery>();
    for (const auto & phase : shuffle_phases)
    {
        ASTs clone_phase;
        for (const auto & ast : phase)
        {
            clone_phase.emplace_back(ast->clone());
        }
        res->shuffle_phases.emplace_back(clone_phase);
    }
    res->final_query = final_query->clone();
    return res;
}

void ASTShufflePhasesSelectQuery::formatImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    final_query->format(settings);
    settings.ostr << "\n";
    if (!shuffle_phases.empty())
    {
        for (int i = shuffle_phases.size() - 1; i >= 0; i--)
        {
            const auto & phase = shuffle_phases[i];
            for (const auto & ast : phase)
            {
                for (int j = 0; j < static_cast<int>(i - shuffle_phases.size()); ++j)
                    settings.ostr << " ";
                ast->format(settings);
                settings.ostr << "\n";
            }

        }
    }
}
}
