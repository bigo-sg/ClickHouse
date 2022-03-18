#pragma  once
#include <Parsers/IAST.h>
namespace DB
{
class ASTShufflePhasesSelectQuery : public IAST
{
public:
    std::vector<ASTs> shuffle_phases;
    ASTPtr final_query;
    String getID(char) const override { return "ASTShufflePhasesSelectQuery"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
