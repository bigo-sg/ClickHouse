#pragma once
#include <Interpreters/StorageDistributedTasksBuilder.h>
#include "Interpreters/StorageDistributedTasksBuilder.h"
#include <Storages/Hive/StorageHiveCluster.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageDistributedTasksBuilder.h>
namespace DB
{
class StorageHiveClusterDistributedTasksBuilder : public IStorageDistributedTaskBuilder
{
public:
    ~StorageHiveClusterDistributedTasksBuilder() override = default;
    DistributedTasks getDistributedTasks(const String & cluster_name, ContextPtr context, ASTPtr ast, StoragePtr storage) override;
};
}
