#pragma once
#include <gpos/memory/CAutoMemoryPool.h>
#include <Optimizer/ICHOptimizer.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <Optimizer/Orca/OrcaMetadataProvider.h>
#include <Optimizer/Orca/OrcaUtil.h>
#include <naucrates/md/CSystemId.h>
#include <gpopt/mdcache/CMDAccessor.h>
namespace DB
{

class OrcaOptimizerRequest : public OptimizerRequest
{
public:
    ~OrcaOptimizerRequest() override = default;
    ContextPtr context;
    ASTPtr query;
};

class OrcaOptimizerResponse : public OptimizerResponse
{
public:
    ~OrcaOptimizerResponse() override = default;

    ASTPtr rewritten_query;
};
/// For convinient usage.
class OrcaOptimizerWrapper : public ICHOptimizer
{
public:
    static constexpr auto NAME = "Orca";
    OrcaOptimizerWrapper();
    ~OrcaOptimizerWrapper() override;
    OptimizerResponsePtr optimize(OptimizerRequestPtr request) override;
    String name() override { return NAME; }

    static void * optimizationTask(void * arg_);

private:
    Poco::Logger * logger = &Poco::Logger::get("OrcaOptimizerWrapper");
    gpos::CAutoMemoryPool mem_pool;
    MemPoolObject<OrcaMetadataProvider> metadata_provider;
    MemPoolObject<gpmd::CSystemIdArray> sysid_array;
    gpopt::CMDAccessor* metadata_accessor = nullptr;

    MemPoolObject<gpmd::CSystemIdArray> getDefaultSystemIdArray();
    gpopt::CMDAccessor* getMetadataAccessor();
};
}
