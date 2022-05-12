#include <Storages/HDFS/HDFSCommon.h>

#if USE_HDFS
#include <filesystem>

#include <Poco/URI.h>
#include <boost/algorithm/string/replace.hpp>
#include <re2/re2.h>

#include <Common/ShellCommand.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
}

static const String HDFS_URL_REGEXP = "^hdfs://[^/]*/.*";


void HDFSBuilderWrapper::loadFromConfig(const String & prefix, bool isUser)
{
    Poco::Util::AbstractConfiguration::Keys keys;

    config.keys(prefix, keys);
    for (const auto & key : keys)
    {
        const String key_path = prefix + "." + key;

        String key_name;
        if (key == "hadoop_kerberos_keytab")
        {
            need_kinit = true;
            hadoop_kerberos_keytab = config.getString(key_path);
            continue;
        }
        else if (key == "hadoop_kerberos_principal")
        {
            need_kinit = true;
            hadoop_kerberos_principal = config.getString(key_path);
            hdfsBuilderSetPrincipal(hdfs_builder, hadoop_kerberos_principal.c_str());
            continue;
        }
        else if (key == "hadoop_kerberos_kinit_command")
        {
            need_kinit = true;
            hadoop_kerberos_kinit_command = config.getString(key_path);
            continue;
        }
        else if (key == "hadoop_security_kerberos_ticket_cache_path")
        {
            if (isUser)
            {
                throw Exception("hadoop.security.kerberos.ticket.cache.path cannot be set per user",
                    ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
            }

            hadoop_security_kerberos_ticket_cache_path = config.getString(key_path);
            // standard param - pass further
        }

        key_name = boost::replace_all_copy(key, "_", ".");

        const auto & [k,v] = keep(key_name, config.getString(key_path));
        hdfsBuilderConfSetStr(hdfs_builder, k.c_str(), v.c_str());
    }
}

String HDFSBuilderWrapper::getKinitCmd()
{

    if (hadoop_kerberos_keytab.empty() || hadoop_kerberos_principal.empty())
    {
        throw Exception("Not enough parameters to run kinit",
            ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    }

    WriteBufferFromOwnString ss;

    String cache_name =  hadoop_security_kerberos_ticket_cache_path.empty() ?
        String() :
        (String(" -c \"") + hadoop_security_kerberos_ticket_cache_path + "\"");

    // command to run looks like
    // kinit -R -t /keytab_dir/clickhouse.keytab -k somebody@TEST.CLICKHOUSE.TECH || ..
    ss << hadoop_kerberos_kinit_command << cache_name <<
        " -R -t \"" << hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal <<
        "|| " << hadoop_kerberos_kinit_command << cache_name << " -t \"" <<
        hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal;
    return ss.str();
}

void HDFSBuilderWrapper::runKinit()
{
    String cmd = getKinitCmd();
    LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "running kinit: {}", cmd);

    std::unique_lock<std::mutex> lck(kinit_mtx);

    auto command = ShellCommand::execute(cmd);
    auto status = command->tryWait();
    if (status)
    {
        throw Exception("kinit failure: " + cmd, ErrorCodes::BAD_ARGUMENTS);
    }
}

void HDFSBuilderWrapper::initialize()
{
    const Poco::URI uri(hdfs_uri);
    const auto & host = uri.getHost();
    auto port = uri.getPort();
    const String path = "//";
    if (host.empty())
        throw Exception("Illegal HDFS URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

    if (get() == nullptr)
        throw Exception("Unable to create builder to connect to HDFS: " +
            uri.toString() + " " + String(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);

    hdfsBuilderConfSetStr(get(), "input.read.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(get(), "input.write.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(get(), "input.connect.timeout", "60000"); // 1 min

    String user_info = uri.getUserInfo();
    String user;
    if (!user_info.empty() && user_info.front() != ':')
    {
        size_t delim_pos = user_info.find(':');
        if (delim_pos != String::npos)
            user = user_info.substr(0, delim_pos);
        else
            user = user_info;

        hdfsBuilderSetUserName(get(), user.c_str());
    }

    hdfsBuilderSetNameNode(get(), host.c_str());
    if (port != 0)
    {
        hdfsBuilderSetNameNodePort(get(), port);
    }

    if (config.has(HDFSBuilderWrapper::CONFIG_PREFIX))
    {
        loadFromConfig(HDFSBuilderWrapper::CONFIG_PREFIX);
    }

    if (!user.empty())
    {
        String user_config_prefix = HDFSBuilderWrapper::CONFIG_PREFIX + "_" + user;
        if (config.has(user_config_prefix))
        {
            loadFromConfig(user_config_prefix, true);
        }
    }

    if (need_kinit)
    {
        runKinit();
    }
}

HDFSFilePool::HDFSFilePool(HDFSFSSharedPtr fs_, const String & hdfs_uri_, const String & hdfs_path_, unsigned pool_size)
    : PoolBase<std::remove_pointer_t<hdfsFile>>(pool_size, &Poco::Logger::get("HDFSFilePool"))
    , fs(std::move(fs_))
    , hdfs_uri(hdfs_uri_)
    , hdfs_path(hdfs_path_)
{
    assert(fs);
}

HDFSFilePool::ObjectPtr HDFSFilePool::allocObject()
{
    auto fin = hdfsOpenFile(fs.get(), hdfs_path.c_str(), O_RDONLY, 0, 0, 0);
    if (fin == nullptr)
        throw Exception(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Unable to open HDFS file: {}. Error: {}",
            hdfs_uri + hdfs_path,
            std::string(hdfsGetLastError()));

    return {fin, [&](hdfsFile f) { hdfsCloseFile(fs.get(), f); }};
}

HDFSFSPool::HDFSFSPool(uint32_t max_items_, HDFSBuilderWrapperPtr builder_)
    : max_items(max_items_), builder(std::move(builder_))
{
    assert(max_items);
    assert(builder && builder->get());

    fses_with_cache.resize(max_items);

    ThreadPool thread_pool(std::min(static_cast<uint32_t>(32), max_items));
    for (size_t i = 0; i < max_items; ++i)
    {
        thread_pool.scheduleOrThrowOnError(
            [this, i]() {
                fses_with_cache[i] = {createSharedHDFSFS(builder->get()), std::make_shared<HDFSFilePoolCache>(file_pool_cache_size)};
            });
    }
    thread_pool.wait();
}

HDFSFSSharedPtr HDFSFSPool::getFS()
{
    // std::lock_guard lock{mutex};
    if (current_index == max_items)
    {
        current_index = 0;
    }
    return fses_with_cache[current_index++].first;
}

HDFSFilePool::Entry HDFSFSPool::getFile(HDFSFSSharedPtr fs, const String & path)
{
    HDFSFilePoolCachePtr file_pool_cache;

    {
        // std::lock_guard lock{mutex};
        for (const auto & pair : fses_with_cache)
        {
            if (fs == pair.first)
            {
                file_pool_cache = pair.second;
                break;
            }
        }
    }

    if (file_pool_cache)
    {
        throw Exception("Can't find hdfsFS in HDFSFSPool", ErrorCodes::LOGICAL_ERROR);
    }
    
    HDFSFilePoolPtr file_pool = file_pool_cache->get(path);
    if (!file_pool)
    {
        file_pool = std::make_shared<HDFSFilePool>(fs, builder->getHDFSUri(), path, file_pool_size);
        file_pool_cache->set(path, file_pool);
    }
    return file_pool->get(-1);
}

std::pair<HDFSFSSharedPtr, HDFSFilePool::Entry> HDFSFSPool::getFSAndFile(const String & path)
{
    auto index = sipHash64(path) % max_items;
    const auto & [fs, cache] = fses_with_cache[index];

    auto file_pool = cache->get(path);
    if (!file_pool)
    {
        file_pool = std::make_shared<HDFSFilePool>(fs, builder->getHDFSUri(), path, file_pool_size);
        cache->set(path, file_pool);
    }
    return {fs, file_pool->get(-1)};
}

HDFSHandlerFactory & HDFSHandlerFactory::instance()
{
    static HDFSHandlerFactory factory;
    return factory;
}

void HDFSHandlerFactory::setEnv(const Poco::Util::AbstractConfiguration & config)
{
    String libhdfs3_conf = config.getString("hdfs.libhdfs3_conf", "");
    if (!libhdfs3_conf.empty())
    {
        if (std::filesystem::path{libhdfs3_conf}.is_relative() && !std::filesystem::exists(libhdfs3_conf))
        {
            const String config_path = config.getString("config-file", "config.xml");
            const auto config_dir = std::filesystem::path{config_path}.remove_filename();
            if (std::filesystem::exists(config_dir / libhdfs3_conf))
                libhdfs3_conf = std::filesystem::absolute(config_dir / libhdfs3_conf);
        }
        setenv("LIBHDFS3_CONF", libhdfs3_conf.c_str(), 1);
    }
}

HDFSBuilderWrapperPtr HDFSHandlerFactory::getBuilder(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(mutex);

    auto it = hdfs_builder_wrappers.find(hdfs_uri);
    if (it == hdfs_builder_wrappers.end())
    {
        auto result = std::make_shared<HDFSBuilderWrapper>(hdfs_uri, config);
        hdfs_builder_wrappers.emplace(hdfs_uri, result);
        return result;
    }
    return it->second;
}

HDFSFSSharedPtr HDFSHandlerFactory::getFS(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config)
{
    auto builder = getBuilder(hdfs_uri, config);
    return getFS(std::move(builder));
}

HDFSFSSharedPtr HDFSHandlerFactory::getFS(HDFSBuilderWrapperPtr builder)
{
    HDFSFSPoolPtr pool;
    const auto hdfs_uri = builder->getHDFSUri();

    {
        std::lock_guard lock(mutex);
        auto it = hdfs_fs_pools.find(hdfs_uri);
        if (it == hdfs_fs_pools.end())
        {
            pool = std::make_shared<HDFSFSPool>(fs_pool_size, std::move(builder));
            hdfs_fs_pools.emplace(hdfs_uri, pool);
        }
        else
        {
            pool = it->second;
        }
    }
    return pool->getFS();
}

std::pair<HDFSFSSharedPtr, HDFSFilePool::Entry> HDFSHandlerFactory::getFSAndFile(HDFSBuilderWrapperPtr builder, const String & path)
{
    HDFSFSPoolPtr pool;
    auto hdfs_uri = builder->getHDFSUri();

    {
        std::lock_guard lock(mutex);
        auto it = hdfs_fs_pools.find(hdfs_uri);
        if (it == hdfs_fs_pools.end())
        {
            pool = std::make_shared<HDFSFSPool>(fs_pool_size, std::move(builder));
            hdfs_fs_pools.emplace(hdfs_uri, pool);
        }
        else
        {
            pool = it->second;
        }
    }
    return pool->getFSAndFile(path);
}

HDFSFSPtr createHDFSFS(hdfsBuilder * builder)
{
    HDFSFSPtr fs(hdfsBuilderConnect(builder));
    if (fs == nullptr)
        throw Exception("Unable to connect to HDFS: " + String(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);

    return fs;
}

HDFSFSSharedPtr createSharedHDFSFS(hdfsBuilder * builder)
{
    HDFSFSSharedPtr fs(hdfsBuilderConnect(builder), detail::HDFSFsDeleter());
    if (fs == nullptr)
        throw Exception("Unable to connect to HDFS: " + String(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);
    
    return fs;
}


String getNameNodeUrl(const String & hdfs_url)
{
    const size_t pos = hdfs_url.find('/', hdfs_url.find("//") + 2);
    String namenode_url = hdfs_url.substr(0, pos) + "/";
    return namenode_url;
}

String getNameNodeCluster(const String &hdfs_url)
{
    auto pos1 = hdfs_url.find("//") + 2;
    auto pos2 = hdfs_url.find('/', pos1);

    return hdfs_url.substr(pos1, pos2 - pos1);
}

void checkHDFSURL(const String & url)
{
    if (!re2::RE2::FullMatch(url, HDFS_URL_REGEXP))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad hdfs url: {}. It should have structure 'hdfs://<host_name>:<port>/<path>'", url);
}

}

#endif
