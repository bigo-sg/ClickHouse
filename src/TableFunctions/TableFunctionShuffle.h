#pragma once
#include <TableFunctions/ITableFunction.h>
#include <Poco/Logger.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>
namespace DB
{
/**
 * Only for inserting chunks into different nodes.
 *
 */
class TableFunctionLocalShuffle : public ITableFunction
{
public:
    static constexpr auto name = "localShuffleStorage";
    static constexpr auto storage_type_name = "StorageLocalShuffle";// but no storage is registered
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    Poco::Logger * logger = &Poco::Logger::get("TableFunctionLocalShuffle");

    String cluster_name;
    String session_id;
    String table_id;
    String table_structure;

    ColumnsDescription columns;

};

class TableFunctionShuffleJoin : public ITableFunction
{
public:
    static constexpr auto name = "shuffleJoinStorage";
    static constexpr auto storage_type_name = "StorageShuffleJoin";// but no storage is registered
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

    ColumnsDescription columns;
    ASTPtr hash_expr_list_ast;
};

class TableFunctionShuffleAggregation : public ITableFunction
{
public:
    static constexpr auto name = "shuffleAggregationStorage";
    static constexpr auto storage_type_name = "StorageShuffleAggregation";// but no storage is registered
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    Poco::Logger * logger = &Poco::Logger::get("TableFunctionShuffleAggregation");

    // followings are args
    String cluster_name;
    String session_id;
    String table_id;
    String table_structure;
    String table_hash_exprs;

    ColumnsDescription columns;
    ASTPtr hash_expr_list_ast;
};

class TableFunctionClosedShuffle : public ITableFunction
{
public:
    static constexpr auto name = "closedShulleStorage";
    static constexpr auto storage_type_name = "ClosedShuffleStorage";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;
private:
    String cluster_name;
    String session_id;
    String table_id;
    ColumnsDescription columns;
};
}
