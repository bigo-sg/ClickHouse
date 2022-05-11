#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <memory>
#include <type_traits>
#include <vector>

#include <hdfs/hdfs.h>
#include <base/types.h>
#include <mutex>

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/PoolBase.h>
#include <Common/LRUCache.h>


namespace DB
{

namespace detail
{
    struct HDFSFsDeleter
    {
        void operator()(hdfsFS fs_ptr)
        {
            hdfsDisconnect(fs_ptr);
            std::cout << "disconnect hdfs fs: trace" << StackTrace().toString() << std::endl;
        }
    };
}


struct HDFSFileInfo
{
    hdfsFileInfo * file_info;
    int length;

    HDFSFileInfo() : file_info(nullptr) , length(0) {}

    HDFSFileInfo(const HDFSFileInfo & other) = delete;
    HDFSFileInfo(HDFSFileInfo && other) = default;
    HDFSFileInfo & operator=(const HDFSFileInfo & other) = delete;
    HDFSFileInfo & operator=(HDFSFileInfo && other) = default;

    ~HDFSFileInfo()
    {
        hdfsFreeFileInfo(file_info, length);
    }
};


class HDFSBuilderWrapper
{
public:
    HDFSBuilderWrapper(const String & hdfs_uri_, const Poco::Util::AbstractConfiguration & config_)
        : hdfs_builder(hdfsNewBuilder()), hdfs_uri(hdfs_uri_), config(config_)
    {
        initialize();
    }

    ~HDFSBuilderWrapper() { hdfsFreeBuilder(hdfs_builder); }

    HDFSBuilderWrapper(const HDFSBuilderWrapper &) = delete;
    HDFSBuilderWrapper(HDFSBuilderWrapper &&) = default;

    HDFSBuilderWrapper & operator=(const HDFSBuilderWrapper &) = delete;

    hdfsBuilder * get() { return hdfs_builder; }

    String getHDFSUri() const { return hdfs_uri; }

private:
    void initialize();

    void loadFromConfig(const String & prefix, bool isUser = false);

    String getKinitCmd();

    void runKinit();

    // hdfs builder relies on an external config data storage
    std::pair<String, String>& keep(const String & k, const String & v)
    {
        return config_stor.emplace_back(std::make_pair(k, v));
    }

    inline static const String CONFIG_PREFIX = "hdfs";
    inline static std::mutex kinit_mtx;

    hdfsBuilder * hdfs_builder;
    const String hdfs_uri;
    const Poco::Util::AbstractConfiguration & config;
    String hadoop_kerberos_keytab;
    String hadoop_kerberos_principal;
    String hadoop_kerberos_kinit_command = "kinit";
    String hadoop_security_kerberos_ticket_cache_path;

    std::vector<std::pair<String, String>> config_stor;
    bool need_kinit{false};
};

using HDFSBuilderWrapperPtr = std::shared_ptr<HDFSBuilderWrapper>;
using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;
using HDFSFSSharedPtr = std::shared_ptr<std::remove_pointer_t<hdfsFS>>;

class HDFSFilePool : public PoolBase<std::remove_pointer_t<hdfsFile>>
{
public:
    using Base = PoolBase<std::remove_pointer_t<hdfsFile>>;
    using ObjectPtr = Base::ObjectPtr;
    using Entry = Base::Entry;

    HDFSFilePool(HDFSFSSharedPtr fs_, const String & hdfs_uri_, const String & hdfs_path_, unsigned pool_size);

    ~HDFSFilePool() override = default;

private:
    ObjectPtr allocObject() override;

    const HDFSFSSharedPtr fs;
    const String hdfs_uri;
    const String hdfs_path;
};

using HDFSFilePtr = HDFSFilePool::ObjectPtr;
using HDFSFilePoolPtr = std::shared_ptr<HDFSFilePool>;
using HDFSFilePoolCache = LRUCache<String, HDFSFilePool>;
using HDFSFilePoolCachePtr = std::shared_ptr<HDFSFilePoolCache>;

class HDFSFSPool : public boost::noncopyable
{
public:
    explicit HDFSFSPool(uint32_t max_items_, HDFSBuilderWrapperPtr builder_);

    ~HDFSFSPool() = default;

    HDFSFSSharedPtr getFS();

    HDFSFilePool::Entry getFile(HDFSFSSharedPtr fs, const String & path);

    std::pair<HDFSFSSharedPtr, HDFSFilePool::Entry> getFSAndFile(const String & path);

private:
    inline static const size_t file_pool_cache_size = 256;
    inline static const unsigned file_pool_size = 8;

    const uint32_t max_items;
    uint32_t current_index{0};
    HDFSBuilderWrapperPtr builder;
    std::vector<std::pair<HDFSFSSharedPtr, HDFSFilePoolCachePtr>> fses_with_cache;

    // std::mutex mutex;
};

using HDFSFSPoolPtr = std::shared_ptr<HDFSFSPool>;


class HDFSHandlerFactory final: public boost::noncopyable
{
public:
    static HDFSHandlerFactory & instance();

    static void setEnv(const Poco::Util::AbstractConfiguration & config);

    HDFSBuilderWrapperPtr getBuilder(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config);

    HDFSFSSharedPtr getFS(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config);

    HDFSFSSharedPtr getFS(HDFSBuilderWrapperPtr builder);

    std::pair<HDFSFSSharedPtr, HDFSFilePool::Entry> getFSAndFile(HDFSBuilderWrapperPtr builder, const String & path);

private:
    inline static const uint32_t fs_pool_size = 32;

    /// Key: hdfs_uri, value: HDFSBuilderWrapperPtr
    std::map<String, HDFSBuilderWrapperPtr> hdfs_builder_wrappers;
    /// Key: hdfs_uri, value: HDFSFSPool
    std::map<String, HDFSFSPoolPtr> hdfs_fs_pools;
    std::mutex mutex;
};

HDFSFSPtr createHDFSFS(hdfsBuilder * builder);
HDFSFSSharedPtr createSharedHDFSFS(hdfsBuilder * builder);
String getNameNodeUrl(const String & hdfs_url);
String getNameNodeCluster(const String & hdfs_url);

/// Check that url satisfy structure 'hdfs://<host_name>:<port>/<path>'
/// and throw exception if it doesn't;
void checkHDFSURL(const String & url);

}
#endif
