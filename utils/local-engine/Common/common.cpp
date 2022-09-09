#include <AggregateFunctions/registerAggregateFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Logger.h>
#include <Formats/registerFormats.h>
#include <Poco/SimpleFileChannel.h>
#include <Poco/Util/MapConfiguration.h>
#include <loggers/Loggers.h>
#include <jni.h>
#include <filesystem>

using namespace DB;
namespace fs = std::filesystem;

#ifdef __cplusplus
extern "C" {
#endif

void registerAllFunctions()
{
    registerFunctions();
    registerAggregateFunctions();
}


static std::string createDirectory(const std::string & file)
{
    auto path = fs::path(file).parent_path();
    if (path.empty())
        return "";
    fs::create_directories(path);
    return path;
};

void init()
{
    static std::once_flag init_flag;
    std::call_once(
        init_flag,
        []()
        {
            /// Load config
            if (!local_engine::SerializedPlanParser::config)
            {
                /// Load Config
                const char * config_path = std::getenv("CLICKHOUSE_CONFIG_PATH");
                if (!config_path || !*config_path)
                    config_path = "config.xml";
                
                if (fs::exists(config_path) && fs::is_regular_file(config_path)) 
                {
                    DB::ConfigProcessor config_processor(config_path, false, true);
                    config_processor.setConfigPath(fs::path(config_path).parent_path());
                    auto loaded_config = config_processor.loadConfig(false);
                    local_engine::SerializedPlanParser::config = loaded_config.configuration;
                }
                else
                {
                    local_engine::SerializedPlanParser::config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
                }
            }
          

            /// Initialize Loggers
            const auto & config = local_engine::SerializedPlanParser::config;
            auto level = config->getString("logger.level", "information");
            if (config->has("logger.log"))
            {
                auto log_path = config->getString("logger.log");
                if (!log_path.empty())
                    createDirectory(log_path);

                local_engine::Logger::initFileLogger(log_path, level);
            }
            else
            {
                local_engine::Logger::initConsoleLogger(level);
            }
            

            /// Initialize settings
            const std::string prefix = "local_engine.";
            auto settings = Settings();
            if (config->has(prefix + "settings"))
            {
                settings.loadSettingsFromConfig(prefix + "settings", *config);
            }
            settings.set("join_use_nulls", true);

            /// Initialize global context
            if (!local_engine::SerializedPlanParser::global_context)
            {
                local_engine::SerializedPlanParser::shared_context = SharedContextHolder(Context::createShared());
                local_engine::SerializedPlanParser::global_context
                    = Context::createGlobal(local_engine::SerializedPlanParser::shared_context.get());
                local_engine::SerializedPlanParser::global_context->makeGlobalContext();
                local_engine::SerializedPlanParser::global_context->setConfig(config);
                local_engine::SerializedPlanParser::global_context->setSettings(settings);

                auto path = config->getString("path", "/");
                local_engine::SerializedPlanParser::global_context->setPath(path);
            }

            registerAllFunctions();
            registerFormats();

#if USE_EMBEDDED_COMPILER
            /// 128 MB
            constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
            CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size_default, compiled_expression_cache_size_default);
#endif
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
