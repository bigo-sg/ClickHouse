#include <boost/algorithm/string/predicate.hpp>
#include "Functions/FunctionHelpers.h"
#include "config.h"

#if USE_MAXMINDDB && USE_SIMDJSON && USE_HDFS

#    include <maxminddb.h>
#    include <Columns/ColumnArray.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionFactory.h>
#    include <Functions/IFunction.h>
#    include <IO/WriteBufferFromFile.h>
#    include <IO/copyData.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/HDFS/HDFSCommon.h>
#    include <Storages/HDFS/ReadBufferFromHDFS.h>
#    include <Storages/MaxMindDB/utils.h>
#    include <hdfs/hdfs.h>
#    include <Common/JSONBuilder.h>
#    include <Common/JSONParsers/SimdJSONParser.h>
#    include <Common/register_objects.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

template <bool is_hello>
class FunctionIP2CountryImpl final : public IFunction, WithContext
{
public:
    static constexpr auto name = is_hello ? "IP2CountryHello" : "IP2CountryBigo";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionIP2CountryImpl>(context_); }

    explicit FunctionIP2CountryImpl(ContextPtr context_) : WithContext(context_) { initialize(context_); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const final { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be String", getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const auto * col_ip = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_ip)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument of function {} must be String", getName());

        auto col_res = ColumnArray::create(ColumnString::create());
        ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
        ColumnArray::Offsets & res_offsets = col_res->getOffsets();
        ColumnString::Chars & res_strings_chars = res_strings.getChars();
        ColumnString::Offsets & res_strings_offsets = res_strings.getOffsets();

        size_t size = col_ip->size();
        res_offsets.reserve(size);
        res_strings_chars.reserve(size * 54);
        res_strings_offsets.reserve(size * 5);

        ColumnArray::Offset current_res_offset = 0;
        ColumnString::Offset current_res_strings_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto ip_ref = col_ip->getDataAt(i);
            std::vector<String> res(5);
            lookup(ip_ref, res[0], res[1], res[2], res[3], res[4]);

            for (const auto & elem : res)
            {
                size_t elem_size = elem.size();
                res_strings_chars.resize(res_strings_chars.size() + elem_size + 1);
                memcpySmallAllowReadWriteOverflow15(&res_strings_chars[current_res_strings_offset], elem.data(), elem_size);
                res_strings_chars[current_res_strings_offset + elem_size] = 0;

                current_res_strings_offset += elem_size + 1;
                res_strings_offsets.push_back(current_res_strings_offset);
            }

            current_res_offset += 5;
            res_offsets.push_back(current_res_offset);
        }

        return col_res;
    }

private:
    static void initialize(ContextPtr context_)
    {
        std::call_once(
            initialize_flag,
            [&context_]()
            {
                auto date_to_str = [](time_t t) -> String
                {
                    struct tm timeinfo;
                    localtime_r(&t, &timeinfo);

                    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                    oss << std::put_time(&timeinfo, "%Y-%m-%d");
                    return oss.str();
                };

                String mmdb_file_name;
                String mmdb_hdfs_path;
                String mmdb_local_path;
                const auto & config = context_->getConfigRef();
                {
                    auto builder = createHDFSBuilder(mmdb_hdfs_uri, config);
                    auto fs = createHDFSFS(builder.get());
                    mmdb_file_name = mmdb_filename_prefix + date_to_str(time(nullptr)) + mmdb_filename_suffix;
                    mmdb_hdfs_path = mmdb_hdfs_dir + "/" + mmdb_file_name;
                    if (hdfsExists(fs.get(), mmdb_hdfs_path.c_str()) != 0)
                    {
                        mmdb_file_name = mmdb_filename_prefix + date_to_str(time(nullptr)) + mmdb_filename_suffix;
                        mmdb_hdfs_path = mmdb_hdfs_dir + "/" + mmdb_file_name;
                        if (hdfsExists(fs.get(), mmdb_hdfs_path.c_str()) != 0)
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR, "Failed to find maxminddb file {} in {}", mmdb_hdfs_path, mmdb_hdfs_uri);
                    }
                }

                if (!std::filesystem::exists(mmdb_local_path))
                {
                    ReadBufferFromHDFS input(mmdb_hdfs_uri, mmdb_hdfs_path, config, {});
                    mmdb_local_path = mmdb_local_dir + "/" + mmdb_file_name;
                    WriteBufferFromFile output(mmdb_local_path);
                    copyData(input, output);
                }

                mmdb_open_status = MMDB_open(mmdb_local_path.c_str(), MMDB_MODE_MMAP, mmdb_ptr.get());
                if (mmdb_open_status != MMDB_SUCCESS)
                {
                    mmdb_ptr.reset();
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Failed to open maxminddb path at: {}: {}",
                        mmdb_local_path,
                        MMDB_strerror(mmdb_open_status));
                }
            });
    }

    static String extractCountry(const SimdJSONParser::Object & obj)
    {
        /// country = obj["country"]["iso_code"]
        SimdJSONParser::Element elem_country;
        bool ok = obj.find("country", elem_country);
        if (!ok || !elem_country.isObject())
            return {};

        SimdJSONParser::Element elem_iso_code;
        ok = elem_country.getObject().find("iso_code", elem_iso_code);
        if (!ok || !elem_iso_code.isString())
            return {};

        return String(elem_iso_code.getString());
    }

    static String extractSubdivision(const SimdJSONParser::Object & obj)
    {
        /// subdivision = obj["subdivisions"][0]["names"]["en"]
        SimdJSONParser::Element elem_subdivisions;
        bool ok = obj.find("subdivisions", elem_subdivisions);
        if (!ok || !elem_subdivisions.isArray())
            return {};

        auto arr_subdivisions = elem_subdivisions.getArray();
        if (arr_subdivisions.size() == 0)
            return {};

        auto elem_subdivision = arr_subdivisions[0];
        if (!elem_subdivision.isObject())
            return {};

        SimdJSONParser::Element elem_names;
        ok = elem_subdivision.getObject().find("names", elem_names);
        if (!ok || !elem_names.isObject())
            return {};

        SimdJSONParser::Element elem_en;
        ok = elem_names.getObject().find("en", elem_en);
        if (!ok || !elem_en.isString())
            return {};

        return String(elem_en.getString());
    }

    static String extractCity(const SimdJSONParser::Object & obj)
    {
        /// city = obj["city"]["names"]["en"]
        SimdJSONParser::Element elem_city;
        bool ok = obj.find("city", elem_city);
        if (!ok || !elem_city.isObject())
            return {};

        SimdJSONParser::Element elem_names;
        ok = elem_city.getObject().find("names", elem_names);
        if (!ok || !elem_names.isObject())
            return {};

        SimdJSONParser::Element elem_en;
        ok = elem_names.getObject().find("en", elem_en);
        if (!ok || !elem_en.isString())
            return {};

        return String(elem_en.getString());
    }

    static std::pair<String, String> extractLatitudeAndLongitude(const SimdJSONParser::Object & obj)
    {
        /// latitude = obj["location"]["latitude"]
        /// longitude = obj["location"]["longitude"]
        SimdJSONParser::Element elem_location;
        bool ok = obj.find("location", elem_location);
        if (!ok || !elem_location.isObject())
            return {};

        String latitude;
        SimdJSONParser::Element elem_latitude;
        ok = elem_location.getObject().find("latitude", elem_latitude);
        if (ok && elem_latitude.isDouble())
            latitude = std::to_string(elem_latitude.getDouble());

        String longitude;
        SimdJSONParser::Element elem_longitude;
        ok = elem_location.getObject().find("longitude", elem_longitude);
        if (ok && elem_longitude.isDouble())
            longitude = std::to_string(elem_longitude.getDouble());

        return {std::move(latitude), std::move(longitude)};
    }

    static void finalizeMMDB(MMDB_s * db)
    {
        if (db != nullptr)
        {
            if (mmdb_open_status == MMDB_SUCCESS)
                MMDB_close(db);

            delete db;
        }
    }

    void lookup(const StringRef & ip, String & country, String & subdivision, String & city, String & latitude, String & longitude) const
    {
        int gai_error, mmdb_error;
        MMDB_lookup_result_s result = MMDB_lookup_string(mmdb_ptr.get(), ip.data, &gai_error, &mmdb_error);
        if (gai_error != 0)
            return;

        if (mmdb_error != MMDB_SUCCESS)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got an error from the maxminddb library because {}", MMDB_strerror(mmdb_error));

        if (!result.found_entry)
            return;

        MMDB_entry_data_list_s * entry_data_list = nullptr;
        int status = MMDB_get_entry_data_list(&result.entry, &entry_data_list);
        if (status != MMDB_SUCCESS)
        {
            MMDB_free_entry_data_list(entry_data_list);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MMDB get_entry_data_list failed because {}", MMDB_strerror(mmdb_error));
        }

        if (!entry_data_list)
            return;

        JSONBuilder::ItemPtr builder;
        std::tie(builder, std::ignore) = dumpMMDBEntryDataList(entry_data_list);
        if (!builder)
            return;

        String json;
        WriteBufferFromString buf(json);
        JSONBuilder::FormatContext format_context{.out = buf};
        FormatSettings format_settings;
        JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
        builder->format(json_format_settings, format_context);
        json.resize(buf.count());

        SimdJSONParser parser;
        SimdJSONParser::Element elem;
        bool ok = parser.parse(json, elem);
        if (!ok || !elem.isObject())
            return;
        auto obj = elem.getObject();

        country = extractCountry(obj);
        subdivision = extractSubdivision(obj);
        city = extractCity(obj);
        std::tie(latitude, longitude) = extractLatitudeAndLongitude(obj);
    }

    inline static std::once_flag initialize_flag;
    inline static const String mmdb_hdfs_uri = is_hello ? "hdfs://hellocluster/" : "hdfs://bigocluster/";
    inline static const String mmdb_hdfs_dir = "/data/services/udf/geoip2";
    inline static const String mmdb_local_dir = ".";
    inline static const String mmdb_filename_prefix = "GeoIP2-City_";
    inline static const String mmdb_filename_suffix = ".mmdb";
    inline static int mmdb_open_status = MMDB_SUCCESS;
    inline static std::unique_ptr<MMDB_s, decltype(&finalizeMMDB)> mmdb_ptr{new MMDB_s(), &finalizeMMDB};
};

REGISTER_FUNCTION(IP2CountryImpl)
{
    factory.registerFunction<FunctionIP2CountryImpl<false>>(); /// IP2CountryBigo
    factory.registerFunction<FunctionIP2CountryImpl<true>>();  /// IP2CountryHello
}

class IP2CountryOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "IP2Country";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<IP2CountryOverloadResolver>(context); }

    explicit IP2CountryOverloadResolver(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &  /*return_type*/) const override
    {
        if (arguments.size() == 1)
            return FunctionFactory::instance().getImpl("IP2CountryBigo", context)->build(arguments);
        else if (arguments.size() == 2)
        {
            if (!isString(arguments[1].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument for function {} must be String", getName());

            const auto * col_const = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
            if (!col_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} must be String", getName());

            auto cluster = col_const->getValue<String>();
            if (boost::iequals(cluster, "hellocluster"))
                return FunctionFactory::instance().getImpl("IP2CountryHello", context)->build({arguments[0]});
            else if (boost::iequals(cluster, "bigocluster"))
                return FunctionFactory::instance().getImpl("IP2CountryBigo", context)->build({arguments[0]});
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} must be 'hellocluster' or 'bigocluster'", getName());
        }

        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
            getName(),
            arguments.size());
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &  /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

private:
    ContextPtr context;
};


REGISTER_FUNCTION(IP2Country)
{
    factory.registerFunction<IP2CountryOverloadResolver>();
}

}
#endif
