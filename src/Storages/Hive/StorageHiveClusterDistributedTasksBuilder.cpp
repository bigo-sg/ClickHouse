#include <memory>
#include <Storages/Hive/StorageHiveClusterDistributedTasksBuilder.h>
#include <Common/ErrorCodes.h>
#include "Interpreters/StorageDistributedTasksBuilder.h"
#include <Storages/Hive/DistributedHiveQueryTaskBuilder.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DistributedTasks StorageHiveClusterDistributedTasksBuilder::getDistributedTasks(const String & cluster_name, ContextPtr context, ASTPtr ast, StoragePtr storage)
{
    auto hive_cluster_storage = std::dynamic_pointer_cast<StorageHiveCluster>(storage);
    if (!hive_cluster_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid storage({})", storage->getName());

    SelectQueryOptions select_options;
    InterpreterSelectQuery interpreter(ast, context, select_options);
    (void)interpreter.execute();
    SelectQueryInfo query_info = interpreter.getQueryInfo();

    IHiveQueryTaskIterateCallback::Arguments args
        = {.cluster_name = cluster_name,
           .storage_settings = hive_cluster_storage->storage_settings,
           .columns = storage->getInMemoryMetadata().getColumns(),
           .context = context,
           .query_info = &query_info,
           .hive_metastore_url = hive_cluster_storage->hive_metastore_url,
           .hive_database = hive_cluster_storage->hive_database,
           .hive_table = hive_cluster_storage->hive_table,
           .partition_by_ast = hive_cluster_storage->partition_by_ast,
           .num_streams = 1};

    DistributedHiveQueryTaskterateCallback task_callback;
    task_callback.setupArgs(args);

    DistributedTasks res;
    auto cluster = context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettings());
    for (const auto & replicas : cluster->getShardsAddresses())
    {
        for (const auto & node : replicas)
        {
            DistributedTask task(node, RemoteQueryExecutor::Extension{.task_iterator = task_callback.buildCallback(node)});
            res.emplace_back(task);
        }
    }
    return res;
}

void registerHiveClusterDistributedTaskBuilder()
{
    StorageDistributedTaskBuilderMaker maker = []()
    {
        return std::make_shared<StorageHiveClusterDistributedTasksBuilder>();
    };
    // HiveClusterStorage need to be the same as StorageHiveCluster::getName();
    StorageDistributedTaskBuilderFactory::getInstance().registerMaker("HiveCluster", maker);
}
}
