#pragma once
#include <memory>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <boost/core/noncopyable.hpp>
#include <Storages/IStorage.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
using DistributedTask = std::pair<Cluster::Address, RemoteQueryExecutor::Extension>;
using DistributedTasks = std::vector<DistributedTask>;
class IStorageDistributedTaskBuilder
{
public:
    virtual ~IStorageDistributedTaskBuilder() = default;
    virtual DistributedTasks getDistributedTasks(const String & cluster_name, ContextPtr context, ASTPtr ast, StoragePtr storage) = 0;
};
using StorageDistributedTaskBuilderPtr = std::shared_ptr<IStorageDistributedTaskBuilder>;

using StorageDistributedTaskBuilderMaker = std::function<StorageDistributedTaskBuilderPtr()>;
class StorageDistributedTaskBuilderFactory : boost::noncopyable
{
public:
    static StorageDistributedTaskBuilderFactory & getInstance();
    void registerMaker(const String & name, StorageDistributedTaskBuilderMaker maker);
    StorageDistributedTaskBuilderPtr getBuilder(const String & name);
protected:
    StorageDistributedTaskBuilderFactory() = default;

private:
    std::map<String, StorageDistributedTaskBuilderMaker> makers;

};

void registerAllStorageDistributedTaskBuilderMakers(StorageDistributedTaskBuilderFactory & instance);
}
