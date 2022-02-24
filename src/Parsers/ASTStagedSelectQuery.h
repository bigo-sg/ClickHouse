#pragma once
#include <Parsers/IAST.h>
namespace DB
{
/**
 * virtual ast for mocking multi-stage query.
 * It is constructed only from MultiStageTreeWriter
 * 
 */
class ASTStagedSelectQuery : public IAST
{
public:
    ASTs staged_insert_queries;
    ASTPtr final_select_query;
    String getID(char) const override { return "StagedSelectQuery"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
}; 
}
