#include <memory>
#include <Optimizer/Orca/OrcOptimizerWrapper.h>
#include <Optimizer/OptimizerFactory.h>
#include <Optimizer/Orca/OrcaEnv.h>
#include <gpos/_api.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "Optimizer/Orca/OrcaUtil.h"
#include <Optimizer/Orca/ASTToQueryDXLAdapter.h>
#include <Optimizer/Orca/OrcaMetadataProvider.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <gpos/common/CAutoRef.h>
#include <gpos/common/CBitSet.h>
#include <gpos/error/CException.h>
#include <gpos/error/CErrorHandlerStandard.h>
#include <gpopt/mdcache/CMDCache.h>
#include <gpopt/optimizer/COptimizer.h>
#include <gpos/memory/CMemoryPool.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

struct OptimizationTaskArg
{
    ContextPtr context;
    ASTPtr query;
    OrcaOptimizerWrapper * optimizer_wrapper;
    OrcaOptimizerResponse * response;
};

void * OrcaOptimizerWrapper::optimizationTask(void * arg_)
{
    OptimizationTaskArg * arg = static_cast<OptimizationTaskArg*>(arg_);
    OrcaOptimizerWrapper * optimizer = arg->optimizer_wrapper;
    optimizer->metadata_provider = OrcaMetadataProviderManager::instance().getMetadataProvider(arg->context, arg->query);
    auto * mem_pool = optimizer->mem_pool.Pmp();
    ASTToQueryDXLAdapter ast2dxl(arg->context, arg->query, optimizer->metadata_provider->getMetadataObjectRetriever(), mem_pool);
    auto dxl_query = ast2dxl.build();
    LOG_DEBUG(
        optimizer->logger,
        "orca query: {}",
        serializeQuery(dxl_query.query.get(), dxl_query.query_output.get(), dxl_query.cte_producers.get()));
#if 1
    gpdxl::CDXLNode * dxl_plan =  nullptr;
    CBitSet * enabled_flags = nullptr, * disabled_flags = nullptr;
    SetTraceflags(mem_pool, OrcaEnv::instance().getTraceFlags().get(), &enabled_flags, &disabled_flags);

    gpos::CErrorHandlerStandard errhdl;
    try
    {
        dxl_plan = gpopt::COptimizer::PdxlnOptimize(
            mem_pool,
            optimizer->getMetadataAccessor(),
            dxl_query.query.get(),
            dxl_query.query_output.get(),
            dxl_query.cte_producers.get(),
            nullptr,
            2,
            1,
            1,
            nullptr,
            OrcaEnv::instance().getOptimizerConfig().get(),
            nullptr);
    }
    catch (gpos::CException & exc)
    {
        errhdl.Process(exc);
        ResetTraceflags(enabled_flags, disabled_flags);
        CRefCount::SafeRelease(enabled_flags);
        CRefCount::SafeRelease(disabled_flags);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Run orca optimization failed");
    }
    LOG_DEBUG(optimizer->logger, "query plan is :{}", serializePlan(dxl_plan));
    dxl_plan->Release();
#endif
    return nullptr;
}

OrcaOptimizerWrapper::OrcaOptimizerWrapper() : mem_pool(gpos::CAutoMemoryPool::ElcNone)
{}

OrcaOptimizerWrapper::~OrcaOptimizerWrapper()
{
    LOG_DEBUG(logger, "~OrcaOptimizerWrapper");
    if (!metadata_accessor)
    {
        GPOS_DELETE(metadata_accessor);
    }
}

OptimizerResponsePtr OrcaOptimizerWrapper::optimize(OptimizerRequestPtr request)
{
    auto rel_req = std::dynamic_pointer_cast<OrcaOptimizerRequest>(request);
    if (!rel_req)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalide request type, OrcaOptimizerRequest is expected");
    }

    OptimizerResponsePtr response = std::make_shared<OrcaOptimizerResponse>();
    OptimizationTaskArg optimization_args
        = {.context = rel_req->context,
           .query = rel_req->query,
           .optimizer_wrapper = this,
           .response = dynamic_cast<OrcaOptimizerResponse *>(response.get())};

    gpos_exec_params exec_params;
    exec_params.func = OrcaOptimizerWrapper::optimizationTask;
    exec_params.arg = &optimization_args;
    exec_params.stack_start = &exec_params;
    exec_params.error_buffer = nullptr;
    exec_params.error_buffer_size = -1;
    exec_params.abort_requested = nullptr;
    if (gpos_exec(&exec_params))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Run optimization task failled. query:{}", queryToString(rel_req->query));
    }

    return response;
}

MemPoolObject<gpmd::CSystemIdArray> OrcaOptimizerWrapper::getDefaultSystemIdArray()
{
    auto * mem_pool_ptr = mem_pool.Pmp();
    if (sysid_array.get() == nullptr)
    {
        sysid_array = MemPoolObject<gpmd::CSystemIdArray>(GPOS_NEW(mem_pool_ptr) gpmd::CSystemIdArray(mem_pool_ptr));
        std::wstring sysid_name = L"GPDB";
        sysid_array->Append(GPOS_NEW(mem_pool_ptr) gpmd::CSystemId(gpmd::IMDId::EmdidGPDB, sysid_name.data(), sysid_name.size()));
    }
    return sysid_array;
}

gpopt::CMDAccessor* OrcaOptimizerWrapper::getMetadataAccessor()
{
    if (metadata_accessor != nullptr)
        return metadata_accessor;
    auto * mem_pool_ptr = mem_pool.Pmp();
    auto sysids = getDefaultSystemIdArray();
    gpos::CAutoRef<gpmd::CMDProviderArray> metadata_providers((GPOS_NEW(mem_pool_ptr) gpmd::CMDProviderArray(mem_pool_ptr)));

    for (size_t i = 0; i < sysids->Size(); ++i)
    {
        metadata_provider->AddRef();
        metadata_providers->Append(metadata_provider.get());
    }
    metadata_accessor
        = GPOS_NEW(mem_pool_ptr) gpopt::CMDAccessor(mem_pool_ptr, gpopt::CMDCache::Pcache(), sysids.get(), metadata_providers.Value());
    LOG_INFO(logger, "metadata_accessor addr:{}", reinterpret_cast<UInt64>(metadata_accessor));
    return metadata_accessor;   
}

void registerOrcaOptimizer(OptimizerFactory & instance)
{
    instance.registerOptimizer(OrcaOptimizerWrapper::NAME, [](){ return std::make_shared<OrcaOptimizerWrapper>(); });
    
}
}
