#include <Interpreters/CollectJoinColumnsVisitor.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <Parsers/queryToString.h>
#include <Common/ErrorCodes.h>
#include <Interpreters/IdentifierSemantic.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int LOGICAL_ERROR;
}
void CollectJoinColumnsMatcher::visit(const ASTPtr & ast_, Data & data_)
{
    //LOG_TRACE9(&Poco::Logger::get("CollectJoinCOlumnsVisitor"), "visit ast : {}", queryToString(ast_));
    if (auto * func = ast_->as<ASTFunction>())
    {
        visit(*func, ast_, data_);
    }
    else if (auto * ident = ast_->as<ASTIdentifier>())
    {
        visit(*ident, ast_, data_);
    }
}
void CollectJoinColumnsMatcher::visit(const ASTFunction & /*func_*/, const ASTPtr & /*ast_*/, Data & /*data_*/)
{
    //LOG_TRACE9(&Poco::Logger::get("CollectJoinColumnsVisitor"), "visit function: {}", queryToString(func_));
}

void CollectJoinColumnsMatcher::visit(const ASTIdentifier & ident_, const ASTPtr & /*ast_*/, Data & data_)
{
    //LOG_TRACE9(&Poco::Logger::get("CollectJoinColumnsVisitor"), "visit ident : {}", queryToString(ast_));
    if (auto best_pos = IdentifierSemantic::chooseTable(ident_, data_.tables, false))
    {
        //LOG_TRACE9(&Poco::Logger::get("CollectJoinColumnsMatcher"), "table pos: {}, cols: {}", *best_pos, data_.tables[*best_pos].columns.toString());
        bool found = false;
        if (*best_pos < data_.tables.size())
        {
            for (const auto & col : data_.tables[*best_pos].columns)
            {
                if (col.name == ident_.shortName())
                {
                    data_.required_columns[*best_pos].push_back(col);
                    found = true;
                    break;
                }
            }
        }
        else {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid table pos: {} for ident: {}", *best_pos, queryToString(ident_));
        }
        if (!found)
        {
            //LOG_TRACE9(&Poco::Logger::get("CollectJoinColumnsMatcher"), "Not found match column for {} - {} ", queryToString(ident_), ident_.name());
        }
    }
    else
    {
        throw Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME, "Position of identifier {} can't be deteminated.", queryToString(ident_));
        ////LOG_TRACE9(&Poco::Logger::get("CollectJoinColumnsMatcher"), "cann't match ident {}. tables columns: {} \n {}",
        //    queryToString(ident_), data_.tables[0].columns.toString(), data_.tables[1].columns.toString());
    }
}

const ASTIdentifier * CollectJoinColumnsMatcher::unrollAliases(const ASTIdentifier * ident_, const Aliases & aliases_)
{
    if (ident_->supposedToBeCompound())
        return ident_;

    UInt32 max_attempts = 100;
    for (auto it = aliases_.find(ident_->name()); it != aliases_.end();)
    {
        const ASTIdentifier * parent = ident_;
        ident_ = it->second->as<ASTIdentifier>();
        if (!ident_)
            break; /// not a column alias
        if (ident_ == parent)
            break; /// alias to itself with the same name: 'a as a'
        if (ident_->supposedToBeCompound())
            break; /// not an alias. Break to prevent cycle through short names: 'a as b, t1.b as a'

        it = aliases_.find(ident_->name());
        if (!max_attempts--)
            throw Exception("Cannot unroll aliases for '" + ident_->name() + "'", ErrorCodes::LOGICAL_ERROR);
    }

    return ident_;
}
}
