#pragma once
#include <TableFunctions/ITableFunction.h>
#include <Poco/Logger.h>
#include "base/types.h"
namespace DB
{
/**
 * Only for inserting chunks into different nodes.
 * 
 */
class DistTableFunctionShuffleJoin : public ITableFunction
{
public:
    static constexpr auto name = "distHashedChunksStorage";
    static constexpr auto storage_type_name = "DistHashedChunksStorage";// but no storage is registered
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    Poco::Logger * logger = &Poco::Logger::get("DistTableFunctionShuffleJoin");

    // followings are args
    String session_id;
    String table_id;
    String table_structure;
    UInt64 active_sinks = 0;

    ColumnsDescription columns;

};

class TableFunctionShuffleJoin : public ITableFunction
{
public:
    static constexpr auto name = "hashedChunksStorage";
    static constexpr auto storage_type_name = "HashedChunksStorage";// but no storage is registered
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    Poco::Logger * logger = &Poco::Logger::get("TableFunctionShuffleJoin");

    // followings are args
    String cluster_name;
    String session_id;
    String table_id;
    String table_structure;
    String table_hash_exprs;
    UInt64 active_sinks = 0;

    ColumnsDescription columns;
    ASTPtr hash_expr_list_ast;
};
}
