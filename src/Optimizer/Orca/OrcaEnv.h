#pragma once
#include <boost/core/noncopyable.hpp>
#include <string>
#include <gpos/memory/CAutoMemoryPool.h>
#include <Optimizer/Orca/OrcaUtil.h>
#include <gpopt/optimizer/COptimizerConfig.h>
#include <gpos/common/CBitSet.h>
#include "Storages/StorageInMemoryMetadata.h"
namespace gpopt
{
class COptimizerConfig;
}
namespace DB
{
class OrcaEnv : public boost::noncopyable
{
public:
    static OrcaEnv & instance();
    void initOnce(const std::string & static_metadata_file, const std::string & config_file);
    ~OrcaEnv();

    MemPoolObject<gpopt::COptimizerConfig> getOptimizerConfig()
    {
        return optimizer_config;
    }

    MemPoolObject<gpos::CBitSet> getTraceFlags()
    {
        return trace_flags;
    }
protected:
    OrcaEnv() = default;
    bool has_init = false;
    CMemoryPool * mem_pool;
    MemPoolObject<gpopt::COptimizerConfig> optimizer_config;
    MemPoolObject<gpos::CBitSet> trace_flags;

    static void * initProviderTask(void * arg_);
    static void * initOptimizerConfigTask(void * arg_);
};
}
