#include <memory>
#include <type_traits>
#include <Optimizer/Orca/OrcaEnv.h>
#include <gpos/_api.h>
#include <gpopt/init.h>
#include <gpopt/optimizer/COptimizerConfig.h>
#include <naucrates/init.h>
#include <naucrates/dxl/parser/CParseHandlerDXL.h>
#include <gpopt/mdcache/CMDCache.h>
#include <Optimizer/Orca/CHToOrcaUtil.h>
#include <Optimizer/Orca/OrcaMetadataProvider.h>
#include <Common/Exception.h>
#include "Optimizer/Orca/OrcaUtil.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
OrcaEnv & OrcaEnv::instance()
{
    static OrcaEnv instance;
    return instance;
}

OrcaEnv::~OrcaEnv()
{
    CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mem_pool);
    gpopt::CMDCache::Shutdown();
}

struct ProviderInitArg
{
    String static_metadata_file;
};

struct OptimizerConfigInitArg
{
    String config_file;
    OrcaEnv * orca_env;
};

void * OrcaEnv::initProviderTask(void * arg_)
{
    ProviderInitArg * arg = static_cast<ProviderInitArg*>(arg_);
    OrcaMetadataProviderManager::instance().initOnce(arg->static_metadata_file);
    return nullptr;
}

void * OrcaEnv::initOptimizerConfigTask(void * arg_)
{
#if 1
    OptimizerConfigInitArg * arg =  static_cast<OptimizerConfigInitArg*>(arg_);
    auto * orca_env = arg->orca_env;
    CAutoRg<CHAR> dxl_buf;
    dxl_buf = CDXLUtils::Read(orca_env->mem_pool, arg->config_file.c_str());
    gpdxl::CParseHandlerDXL * parse_handler_dxl = CDXLUtils::GetParseHandlerForDXLString(orca_env->mem_pool, dxl_buf.Rgt(), nullptr);
    CAutoP<CParseHandlerDXL> parse_handler_dxl_wrapper(parse_handler_dxl);
    orca_env->optimizer_config = MemPoolObject<gpopt::COptimizerConfig>(parse_handler_dxl->GetOptimizerConfig());
    orca_env->trace_flags = MemPoolObject<gpos::CBitSet>(parse_handler_dxl->Pbs());
#endif
    return nullptr;
}

void OrcaEnv::initOnce(const std::string & static_metadata_file, const std::string & config_file)
{
    if (has_init)
        return;
    struct gpos_init_params gpos_params = {nullptr};
    gpos_init(&gpos_params);
    gpdxl_init();
    gpopt_init();
    InitDXL();
    gpopt::CMDCache::Init();
    
    gpos::CAutoMemoryPool mem_pool_holder(gpos::CAutoMemoryPool::ElcNone);
    mem_pool = mem_pool_holder.Detach();
    
    ProviderInitArg provider_task_args;
    provider_task_args.static_metadata_file = static_metadata_file;
    gpos_exec_params exec_params;
    exec_params.func = OrcaEnv::initProviderTask;
    exec_params.arg = &provider_task_args;
    exec_params.stack_start = &exec_params;
    exec_params.error_buffer = nullptr;
    exec_params.error_buffer_size = -1;
    exec_params.abort_requested = nullptr;
    if (gpos_exec(&exec_params))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Init orca environment failed");
    }

    OptimizerConfigInitArg optimizer_config_args;
    optimizer_config_args.config_file = config_file;
    optimizer_config_args.orca_env = this;
    exec_params.func = OrcaEnv::initOptimizerConfigTask;
    exec_params.arg = &optimizer_config_args;
    exec_params.stack_start = &exec_params;
    exec_params.error_buffer = nullptr;
    exec_params.error_buffer_size = -1;
    exec_params.abort_requested = nullptr;
    if (gpos_exec(&exec_params))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Init orca environment failed");
    }

    has_init = true;
}
}
