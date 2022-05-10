#pragma once
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>

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
    static NamesAndTypesList toNamesAndTypesList(const std::vector<ColumnWithDetailNameAndType> & columns);
    std::vector<String> splitedFullName() const;
};
using ColumnWithDetailNameAndTypes = std::vector<ColumnWithDetailNameAndType>;
class ASTAnalyzeUtil
{
public:
    static bool hasGroupByRecursively(const ASTPtr & ast);
    static bool hasGroupBy(const ASTPtr & ast);
    static bool hasGroupByRecursively(const IAST * ast);
    static bool hasGroupBy(const IAST * ast);

    //static bool hasAggregationColumnRecursively(ASTPtr ast);
    static bool hasAggregationColumn(const ASTPtr & ast);
    static bool hasAggregationColumn(const IAST * ast);
    static bool hasAggregationColumnRecursively(const ASTPtr & ast);
    static bool hasAggregationColumnRecursively(const IAST * ast);
    static String tryGetTableExpressionAlias(const ASTTableExpression * table_expr);

};


class ShuffleTableIdGenerator
{
public:
    ShuffleTableIdGenerator():id(0){}
    inline UInt32 nextId() { return id++; }
private:
    UInt32 id;
};
using ShuffleTableIdGeneratorPtr = std::shared_ptr<ShuffleTableIdGenerator>;
}
