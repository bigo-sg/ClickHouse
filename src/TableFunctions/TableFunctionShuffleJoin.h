#pragma once
#include <TableFunctions/ITableFunction.h>
#include <Poco/Logger.h>
namespace DB
{
class DistTableFunctionShuffleJoin : public ITableFunction
{
public:
    static constexpr auto name = "dist_hashed_chunks_storage";
    static constexpr auto storage_type_name = "dist_hashed_chunks_storage";
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

    ColumnsDescription columns;

};

class TableFunctionShuffleJoin : public ITableFunction
{
public:
    static constexpr auto name = "hashed_chunks_storage";
    static constexpr auto storage_type_name = "hashed_chunks_storage";
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
    String session_id;
    String table_id;
    String table_structure;
    String table_hash_exprs;

    ColumnsDescription columns;

};
}
