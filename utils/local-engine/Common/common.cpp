#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Logger.h>
#include <Formats/registerFormats.h>
#include <Poco/SimpleFileChannel.h>
#include <loggers/Loggers.h>
#include <jni.h>

using namespace DB;
#ifdef __cplusplus
extern "C" {
#endif

void registerAllFunctions()
{
    registerFunctions();
    registerAggregateFunctions();
}

void init()
{
    static std::once_flag init_flag;
    std::call_once(
        init_flag,
        []()
        {
            /*
            /// Load Config
            const char * config_path = std::getenv("CLICKHOUSE_CONFIG_PATH");
            if (!config_path || !*config_path)
                config_path = "config.xml";

            DB::ConfigProcessor config_processor(config_path, false, true);
            config_processor.setConfigPath(fs::path(config_path).parent_path());
            auto loaded_config = config_processor.loadConfig(false);
            auto & config = loaded_config.configuration;

            /// Initialize Loggers
            auto level = Poco::Logger::parseLevel(config->getString("logger.level", "trace"));
            auto log_path = config->getString("logger.log", "/tmp/log/clickhouse-server.log");
            Poco::Logger::root().setLevel(level);
            Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::SimpleFileChannel>(new Poco::SimpleFileChannel(log_path)));


            if (!local_engine::SerializedPlanParser::global_context)
            {
                local_engine::SerializedPlanParser::shared_context = SharedContextHolder(Context::createShared());
                local_engine::SerializedPlanParser::global_context
                    = Context::createGlobal(local_engine::SerializedPlanParser::shared_context.get());
                local_engine::SerializedPlanParser::global_context->makeGlobalContext();
                local_engine::SerializedPlanParser::global_context->setConfig(config);
                local_engine::SerializedPlanParser::global_context->setSetting("join_use_nulls", true);

                auto path = config->getString("path", "/tmp/data");
                local_engine::SerializedPlanParser::global_context->setPath(path);
            }

            registerAllFunctions();
            registerFormats();

#if USE_EMBEDDED_COMPILER
            /// 128 MB
            constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
            CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size_default, compiled_expression_cache_size_default);
#endif
        */
        }

    );
}

char * createExecutor(std::string plan_string)
{
    auto context = Context::createCopy(local_engine::SerializedPlanParser::global_context);
    local_engine::SerializedPlanParser parser(context);
    auto query_plan = parser.parse(plan_string);
    local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context);
    executor->execute(std::move(query_plan));
    return reinterpret_cast<char* >(executor);
}

bool executorHasNext(char * executor_address)
{
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
}



#ifdef __cplusplus
}
#endif
