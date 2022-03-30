#include <Interpreters/StorageDistributedTasksBuilder.h>
#include <Common/ErrorCodes.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
StorageDistributedTaskBuilderFactory & StorageDistributedTaskBuilderFactory::getInstance()
{
    static StorageDistributedTaskBuilderFactory instance;
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

void registerHiveClusterDistributedTaskBuilder();

void registerAllStorageDistributedTaskBuilderMakers()
{
    registerHiveClusterDistributedTaskBuilder();
}
}
