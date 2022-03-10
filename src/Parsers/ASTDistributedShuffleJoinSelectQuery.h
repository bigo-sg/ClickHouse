#pragma once
#include <Parsers/IAST.h>
namespace DB
{
/**
 * virtual ast for mocking multi-stage query.
 * It is constructed only from MultiStageTreeWriter
 * 
 */
class ASTDistributedShuffleJoinSelectQuery : public IAST
{
public:
    ASTs shuffle_queries;
    ASTPtr select_query;
    String getID(char) const override { return "DistributedShuffleJoinSelectQuery"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
}; 
}
