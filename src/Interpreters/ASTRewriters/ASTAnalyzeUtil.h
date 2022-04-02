#pragma  once
#include <Parsers/IAST_fwd.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include "Core/NamesAndTypes.h"

namespace DB
{
struct ColumnWithDetailNameAndType
{
    String full_name;
    String short_name;
    String alias_name;
    DataTypePtr type;
    String toString() const;

    static void makeAliasByFullName(std::vector<ColumnWithDetailNameAndType> & columns);
    //static NamesAndTypesList toNamesAndTypesList(std::vector<ColumnWithDetailNameAndType> & columns);
    std::vector<String> splitedFullName() const;
};
using ColumnWithDetailNameAndTypes = std::vector<ColumnWithDetailNameAndType>;
class ASTAnalyzeUtil
{
public:
    static bool hasGroupByRecursively(ASTPtr ast);
    static bool hasGroupBy(ASTPtr ast);

    //static bool hasAggregationColumnRecursively(ASTPtr ast);
    static bool hasAggregationColumn(ASTPtr ast);

};

class ASTBuildUtil
{
public:
    static void updateSelectLeftTableBySubquery(ASTSelectQuery * select, ASTSelectWithUnionQuery * subquery, const String & alias = "");
    static void updateJoinedSelectTables(ASTSelectQuery *select, ASTTableExpression *left_table, ASTTableExpression *right_table, ASTTableJoin * join);
};
}
