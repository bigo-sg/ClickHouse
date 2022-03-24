#pragma once
#include <Parsers/IAST.h>
#include "Parsers/IAST_fwd.h"
/**
 * For making one query into multi queries。 Eache query is a tree node. Current query will be executed only when 
 * all its children nodes finshed execution。
 * 
 * Mostly are designed for distributed shuffle join
 * 
 * Be careful, ASTTreeQuery cannot be an inner ast in ASTSelectQuery or ASTSelectWithUnionQuery. But inner ast in
 * ASTTreeQuery could be ASTSelectQuery, ASTSelectWithUnionQuery or ASTTreeQuery.
 */
namespace DB
{
class ASTTreeQuery : public IAST
{
public:
    ASTs input_asts; // be relied by output_ast
    ASTPtr output_ast; // rely on input_asts
    String getID(char) const override { return "ASTTreeQeury"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
