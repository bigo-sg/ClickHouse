#pragma once
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
/**
 * For making one query into multi queries。 Eache query is a tree node. Current query will be executed only when
 * all its children nodes finshed execution。
 *
 * Mostly are designed for distributed shuffle join
 *
 * Be careful, ASTStageQuery cannot be an inner ast in ASTSelectQuery or ASTSelectWithUnionQuery. But inner ast in
 * ASTStageQuery could be ASTSelectQuery, ASTSelectWithUnionQuery or ASTStageQuery.
 */
namespace DB
{
class ASTStageQuery : public IAST
{
public:
    ASTs upstream_queries; // be relied by current_query
    ASTPtr current_query; // rely on upstream_queries
    String getID(char) const override { return "ASTStageQuery"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    static ASTPtr make(ASTPtr current_query, ASTs upstream_queries);
};
}
