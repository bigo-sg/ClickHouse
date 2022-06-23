#include <mutex>
#include <Interpreters/StorageDistributedTasksBuilder.h>
#include <Common/ErrorCodes.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::once_flag init_builder_flag;
void registerAllStorageDistributedTaskBuilderMakers();
StorageDistributedTaskBuilderFactory & StorageDistributedTaskBuilderFactory::getInstance()
{
    static StorageDistributedTaskBuilderFactory instance;
    std::call_once(init_builder_flag, [](){ registerAllStorageDistributedTaskBuilderMakers(instance); });
    return instance;
}

void StorageDistributedTaskBuilderFactory::registerMaker(const String & name, StorageDistributedTaskBuilderMaker maker)
{
    auto iter = makers.find(name);
    if (iter != makers.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicated maker : {}", name);
    }
    makers[name] = maker;
}

StorageDistributedTaskBuilderPtr StorageDistributedTaskBuilderFactory::getBuilder(const String & name)
{
    auto iter = makers.find(name);
    if (iter == makers.end())
        return nullptr;
    return iter->second();
}

void registerHiveClusterTasksBuilder(StorageDistributedTaskBuilderFactory & instance);
void registerAllStorageDistributedTaskBuilderMakers(StorageDistributedTaskBuilderFactory & instance)
{
    registerHiveClusterTasksBuilder(instance);
}
}
