#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>

#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/getLeastSupertype.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include "JSONEachRowRowInputFormat.h"
#include <simdjson.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

enum
{
    UNKNOWN_FIELD = size_t(-1),
    NESTED_FIELD = size_t(-2)
};

}


JSONEachRowRowInputFormat::JSONEachRowRowInputFormat(
    ReadBuffer & in_,
    const Block & header_,
    Params params_,
    const FormatSettings & format_settings_,
    bool yield_strings_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , prev_positions(header_.columns())
    , yield_strings(yield_strings_)
    , format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    name_map = header.getNamesToIndexesMap();
    if (format_settings_.import_nested_json)
    {
        for (size_t i = 0; i != header.columns(); ++i)
        {
            const StringRef column_name = header.getByPosition(i).name;
            const auto split = Nested::splitName(column_name.toView());
            if (!split.second.empty())
            {
                const StringRef table_name(column_name.data, split.first.size());
                name_map[table_name] = NESTED_FIELD;
            }
        }
    }
}

const String & JSONEachRowRowInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

inline size_t JSONEachRowRowInputFormat::columnIndex(StringRef name, size_t key_index)
{
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.

    if (prev_positions.size() > key_index
        && prev_positions[key_index] != Block::NameMap::const_iterator{}
        && name == prev_positions[key_index]->first)
    {
        return prev_positions[key_index]->second;
    }
    else
    {
        const auto it = name_map.find(name);
        if (it != name_map.end())
        {
            if (key_index < prev_positions.size())
                prev_positions[key_index] = it;

            return it->second;
        }
        else
            return UNKNOWN_FIELD;
    }
}

/** Read the field name and convert it to column name
  *  (taking into account the current nested name prefix)
  * Resulting StringRef is valid only before next read from buf.
  */
StringRef JSONEachRowRowInputFormat::readColumnName(ReadBuffer & buf)
{
    // This is just an optimization: try to avoid copying the name into current_column_name

    if (nested_prefix_length == 0 && !buf.eof() && buf.position() + 1 < buf.buffer().end())
    {
        char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());

        if (next_pos != buf.buffer().end() && *next_pos != '\\')
        {
            /// The most likely option is that there is no escape sequence in the key name, and the entire name is placed in the buffer.
            assertChar('"', buf);
            StringRef res(buf.position(), next_pos - buf.position());
            buf.position() = next_pos + 1;
            return res;
        }
    }

    current_column_name.resize(nested_prefix_length);
    readJSONStringInto(current_column_name, buf);
    return current_column_name;
}

void JSONEachRowRowInputFormat::skipUnknownField(StringRef name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing JSONEachRow format: {}", name_ref.toString());

    skipJSONField(*in, name_ref);
}

void JSONEachRowRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing JSONEachRow format: {}", columnName(index));

    seen_columns[index] = true;
    const auto & type = getPort().getHeader().getByPosition(index).type;
    const auto & serialization = serializations[index];
    read_columns[index] = JSONUtils::readField(*in, *columns[index], type, serialization, columnName(index), format_settings, yield_strings);
}

inline bool JSONEachRowRowInputFormat::advanceToNextKey(size_t key_index)
{
    skipWhitespaceIfAny(*in);

    if (in->eof())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream while parsing JSONEachRow format");
    else if (*in->position() == '}')
    {
        ++in->position();
        return false;
    }

    if (key_index > 0)
        JSONUtils::skipComma(*in);
    return true;
}

void JSONEachRowRowInputFormat::readJSONObject(MutableColumns & columns)
{
    assertChar('{', *in);
    // const auto & header = getPort().getHeader();

    for (size_t key_index = 0; advanceToNextKey(key_index); ++key_index)
    {
        StringRef name_ref = readColumnName(*in);
        const size_t column_index = columnIndex(name_ref, key_index);

        if (unlikely(ssize_t(column_index) < 0))
        {
            /// name_ref may point directly to the input buffer
            /// and input buffer may be filled with new data on next read
            /// If we want to use name_ref after another reads from buffer, we must copy it to temporary string.

            current_column_name.assign(name_ref.data, name_ref.size);
            name_ref = StringRef(current_column_name);

            JSONUtils::skipColon(*in);

            if (column_index == UNKNOWN_FIELD)
                skipUnknownField(name_ref);
            else if (column_index == NESTED_FIELD)
                readNestedData(name_ref.toString(), columns);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value of column_index");
        }
        else
        {
            JSONUtils::skipColon(*in);
            readField(column_index, columns);
        }
    }
}

void JSONEachRowRowInputFormat::readNestedData(const String & name, MutableColumns & columns)
{
    current_column_name = name;
    current_column_name.push_back('.');
    nested_prefix_length = current_column_name.size();
    readJSONObject(columns);
    nested_prefix_length = 0;
}


bool JSONEachRowRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!allow_new_rows)
        return false;
    skipWhitespaceIfAny(*in);

    bool is_first_row = getRowNum() == 0;
    if (checkEndOfData(is_first_row))
        return false;

    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    nested_prefix_length = 0;
    readRowStart(columns);
    readJSONObject(columns);

    const auto & header = getPort().getHeader();
    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    /// Return info about defaults set.
    /// If defaults_for_omitted_fields is set to 0, we should just leave already inserted defaults.
    if (format_settings.defaults_for_omitted_fields)
        ext.read_columns = read_columns;
    else
        ext.read_columns.assign(read_columns.size(), true);

    return true;
}

bool JSONEachRowRowInputFormat::checkEndOfData(bool is_first_row)
{
    /// We consume ',' or '\n' before scanning a new row, instead scanning to next row at the end.
    /// The reason is that if we want an exact number of rows read with LIMIT x
    /// from a streaming table engine with text data format, like File or Kafka
    /// then seeking to next ';,' or '\n' would trigger reading of an extra row at the end.

    /// Semicolon is added for convenience as it could be used at end of INSERT query.
    if (!in->eof())
    {
        /// There may be optional ',' (but not before the first row)
        if (!is_first_row && *in->position() == ',')
            ++in->position();
        else if (!data_in_square_brackets && *in->position() == ';')
        {
            /// ';' means the end of query (but it cannot be before ']')
            allow_new_rows = false;
            return true;
        }
        else if (data_in_square_brackets && *in->position() == ']')
        {
            /// ']' means the end of query
            allow_new_rows = false;
            return true;
        }
    }

    skipWhitespaceIfAny(*in);
    return in->eof();
}


void JSONEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

void JSONEachRowRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    nested_prefix_length = 0;
    read_columns.clear();
    seen_columns.clear();
    prev_positions.clear();
    allow_new_rows = true;
}

void JSONEachRowRowInputFormat::readPrefix()
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(*in);
    data_in_square_brackets = JSONUtils::checkAndSkipArrayStart(*in);
}

void JSONEachRowRowInputFormat::readSuffix()
{
    skipWhitespaceIfAny(*in);
    if (data_in_square_brackets)
        JSONUtils::skipArrayEnd(*in);

    if (!in->eof() && *in->position() == ';')
    {
        ++in->position();
        skipWhitespaceIfAny(*in);
    }
    assertEOF(*in);
}

size_t JSONEachRowRowInputFormat::countRows(size_t max_block_size)
{
    if (unlikely(!allow_new_rows))
        return 0;

    size_t num_rows = 0;
    bool is_first_row = getRowNum() == 0;
    skipWhitespaceIfAny(*in);
    while (num_rows < max_block_size && !checkEndOfData(is_first_row))
    {
        skipRowStart();
        JSONUtils::skipRowForJSONEachRow(*in);
        ++num_rows;
        is_first_row = false;
        skipWhitespaceIfAny(*in);
    }

    return num_rows;
}

#if USE_SIMDJSON
SIMDJSONEachRowRowInputFormat::SIMDJSONEachRowRowInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_,  bool yield_strings_ [[maybe_unused]])
    : IRowInputFormat(header_, in_, std::move(params_))
    , format_settings(format_settings_)
{
}

bool SIMDJSONEachRowRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!allow_new_rows)
        return false;
    skipWhitespaceIfAny(*in);
    bool is_first_row = getRowNum() == 0;
    if (checkEndOfData(is_first_row))
        return false;
    nextLine();
    const auto & header = getPort().getHeader();
    simdjson::ondemand::parser parser;
    auto document = parser.iterate(current_raw_line);
    size_t column_index = 0;
    std::vector<UInt8> read_columns(header.columns(), false);
    SIMDJSONValueToCHValue value_parser(format_settings);
    for (const auto & col : header.getColumnsWithTypeAndName())
    {
        auto result = document[col.name];
        auto & dst_col = columns[column_index];
        read_columns[column_index] = value_parser.readField(dst_col, col.type, result);
        column_index += 1;
    }
    /// Return info about defaults set.
    /// If defaults_for_omitted_fields is set to 0, we should just leave already inserted defaults.
    if (format_settings.defaults_for_omitted_fields)
        ext.read_columns = read_columns;
    else
        ext.read_columns.assign(read_columns.size(), true);

    return true;
}

void SIMDJSONEachRowRowInputFormat::nextLine()
{
    current_raw_line.clear();
    while (!in->eof())
    {
        char * prev_pos = in->position();
        char * next_pos = find_first_symbols<'\n'>(in->position(), in->buffer().end());
        in->position() = next_pos;

        if (!in->hasPendingData())
        {
            current_raw_line.append(prev_pos, in->buffer().end() - prev_pos);
            continue;
        }

        if (*in->position() == '\n')
        {
            current_raw_line.append(prev_pos, next_pos - prev_pos);
            ++in->position();
            break;
        }
    }
}

bool SIMDJSONEachRowRowInputFormat::checkEndOfData(bool is_first_row)
{
    if (!in->eof())
    {
        if (!is_first_row && *in->position() == ',')
            ++in->position();
        else if (!data_in_square_brackets && *in->position() == ';')
        {
            /// ';' means the end of query (but it cannot be before ']')
            allow_new_rows = false;
            return true;
        }
        else if (data_in_square_brackets && *in->position() == ']')
        {
            /// ']' means the end of query
            allow_new_rows = false;
            return true;
        }
    }
    skipWhitespaceIfAny(*in);
    return in->eof();
}

void SIMDJSONEachRowRowInputFormat::resetParser()
{
    current_raw_line = "";
    allow_new_rows = true;
    data_in_square_brackets = false;
}

void SIMDJSONEachRowRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
    data_in_square_brackets = JSONUtils::checkAndSkipArrayStart(*in);
}

void SIMDJSONEachRowRowInputFormat::readSuffix()
{
    skipWhitespaceIfAny(*in);
    if (data_in_square_brackets)
        JSONUtils::skipArrayEnd(*in);

    if (!in->eof() && *in->position() == ';')
    {
        ++in->position();
        skipWhitespaceIfAny(*in);
    }
    assertEOF(*in);
}

void SIMDJSONEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

size_t SIMDJSONEachRowRowInputFormat::countRows(size_t max_block_size)
{
    if (unlikely(!allow_new_rows))
        return 0;

    size_t num_rows = 0;
    bool is_first_row = getRowNum() == 0;
    skipWhitespaceIfAny(*in);
    while (num_rows < max_block_size && !checkEndOfData(is_first_row))
    {
        JSONUtils::skipRowForJSONEachRow(*in);
        ++num_rows;
        is_first_row = false;
        skipWhitespaceIfAny(*in);
    }

    return num_rows;
}

bool SIMDJSONValueToCHValue::readField(MutableColumnPtr & col, DataTypePtr type, simdjson::simdjson_result<simdjson::ondemand::value> value)
{
    /// hanndle null or missing value.
    auto error_code = value.error();
    if (error_code == simdjson::error_code::NO_SUCH_FIELD || value.is_null())
    {
        if (insert_default_as_missing)
        {
            col->insertDefault();
            return true;
        }
        else
            return false;
    }
    bool res = false;
    auto nested_type = type;
    if (const DataTypeNullable * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        nested_type = type_nullable->getNestedType();
    }
    WhichDataType which(nested_type);

    auto json_type = value.type().value();
    if (which.isUInt8() && isBool(nested_type))
    {
        if (json_type == simdjson::ondemand::json_type::boolean)
        {
            auto bool_val = value.get_bool();
            col->insert(bool_val.value());
            res = true;
        }
        else if (json_type == simdjson::ondemand::json_type::number)
        {
            auto int64_val = value.get_uint64().value();
            if (int64_val > 1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected value {} for boolean field", int64_val);
            bool is_true = int64_val != 0;
            col->insert(is_true);
            res = true;
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected JSON type {} for boolean field", json_type);
        }
    }
    else if (which.isNativeUInt())
    {
        if (json_type == simdjson::ondemand::json_type::number)
        {
            auto uint64_val = value.get_uint64().value();
            col->insert(uint64_val);
        }
        else if (json_type == simdjson::ondemand::json_type::string)
        {
            auto uint64_val = value.get_uint64_in_string();
            col->insert(uint64_val.value());
        }
        else if (json_type == simdjson::ondemand::json_type::boolean)
        {
            auto bool_val = value.get_bool().value();
            col->insert(bool_val);
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected JSON type {} for number field", json_type);
        }
        res = true;
    }
    else if (which.isNativeInt())
    {
        if (json_type == simdjson::ondemand::json_type::number)
        {
            auto int64_val = value.get_int64();
            col->insert(int64_val.value());
        }
        else if (json_type == simdjson::ondemand::json_type::string)
        {
            auto int64_val = value.get_int64_in_string();
            col->insert(int64_val.value());
        }
        else if (json_type == simdjson::ondemand::json_type::boolean)
        {
            auto bool_val = value.get_bool().value();
            col->insert(bool_val);
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected JSON type {} for number field", json_type);
        }
        res = true;

    }
    else if (which.isString())
    {
        col->insert(value.raw_json().value());
        res = true;
    }
    else if (which.isArray())
    {
        res = readArrayField(col, nested_type, value);
    }
    else if (which.isTuple())
    {
        res = readTupleField(col, nested_type, value);
    }
    else
    {
        res = readGeneralField(col, nested_type, value);
    }
    return res;
}

bool SIMDJSONValueToCHValue::readArrayField(MutableColumnPtr & col, DataTypePtr type, simdjson::simdjson_result<simdjson::ondemand::value> value)
{
    auto json_array = value.get_array();
    auto & array_col = typeid_cast<ColumnArray &>(*col);
    auto & offsets = array_col.getOffsets();
    offsets.push_back(offsets.back() + json_array.count_elements());
    auto nested_col = array_col.getData().assumeMutable();
    auto & array_ty = typeid_cast<const DataTypeArray &>(*type);
    auto nested_ty = array_ty.getNestedType();
    for (auto element : json_array)
    {
        readField(nested_col, nested_ty, element);
    }
    return true;
}

bool SIMDJSONValueToCHValue::readTupleField(MutableColumnPtr & col, DataTypePtr type, simdjson::simdjson_result<simdjson::ondemand::value> value)
{
    auto & tuple_col = typeid_cast<ColumnTuple &>(*col);
    auto tuple_size = tuple_col.tupleSize();
    auto & tuple_type = typeid_cast<const DataTypeTuple &>(*type);
    const auto & names = tuple_type.getElementNames();
    if (names.empty())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Tuple must have named fields");
    }
    const auto & types = tuple_type.getElements();
    auto object = value.get_object();
    for (size_t i = 0; i < tuple_size; ++i)
    {
        auto nested_col = tuple_col.getColumn(i).assumeMutable();
        auto & nested_type = types[i];
        auto result = object.find_field(names[i]);
        readField(nested_col, nested_type, result);
    }

    return true;
}

bool SIMDJSONValueToCHValue::readGeneralField(MutableColumnPtr & col, DataTypePtr type, simdjson::simdjson_result<simdjson::ondemand::value> value)
{
    auto raw_str = value.raw_json().value();
    ReadBufferFromString buf(raw_str);
    auto serialization = type->getDefaultSerialization();
    serialization->deserializeTextJSON(*col, buf, format_settings);
    return true;
}
#endif

JSONEachRowSchemaReader::JSONEachRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowWithNamesSchemaReader(in_, format_settings_)
{
}

NamesAndTypesList JSONEachRowSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if (first_row)
    {
        skipBOMIfExists(in);
        data_in_square_brackets = JSONUtils::checkAndSkipArrayStart(in);
        first_row = false;
    }
    else
    {
        skipWhitespaceIfAny(in);
        /// If data is in square brackets then ']' means the end of data.
        if (data_in_square_brackets && checkChar(']', in))
            return {};

        /// ';' means end of data.
        if (checkChar(';', in))
            return {};

        /// There may be optional ',' between rows.
        checkChar(',', in);
    }

    skipWhitespaceIfAny(in);
    if (in.eof())
    {
        eof = true;
        return {};
    }

    return JSONUtils::readRowAndGetNamesAndDataTypesForJSONEachRow(in, format_settings, &inference_info);
}

void JSONEachRowSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesIfNeeded(type, new_type, format_settings, &inference_info);
}

void JSONEachRowSchemaReader::transformTypesFromDifferentFilesIfNeeded(DB::DataTypePtr & type, DB::DataTypePtr & new_type)
{
    transformInferredJSONTypesFromDifferentFilesIfNeeded(type, new_type, format_settings);
}

void JSONEachRowSchemaReader::transformFinalTypeIfNeeded(DataTypePtr & type)
{
    transformFinalInferredJSONTypeIfNeeded(type, format_settings, &inference_info);
}

void registerInputFormatJSONEachRow(FormatFactory & factory)
{
    auto register_format = [&](const String & format_name, bool json_strings)
    {
        factory.registerInputFormat(format_name, [json_strings](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<JSONEachRowRowInputFormat>(buf, sample, std::move(params), settings, json_strings);
        });
    };

    #if USE_SIMDJSON
    auto register_format_simd = [&](const String & format_name, bool json_strings)
    {
        factory.registerInputFormat(format_name, [json_strings](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<SIMDJSONEachRowRowInputFormat>(buf, sample, std::move(params), settings, json_strings);
        });
    };
    register_format_simd("JSONEachRow", false);
    register_format_simd("JSONLines", false);
    register_format_simd("NDJSON", false);
    #else
    register_format("JSONEachRow", false);
    register_format("JSONLines", false);
    register_format("NDJSON", false);
    #endif

    factory.registerFileExtension("ndjson", "JSONEachRow");
    factory.registerFileExtension("jsonl", "JSONEachRow");

    register_format("JSONStringsEachRow", true);

    factory.markFormatSupportsSubsetOfColumns("JSONEachRow");
    factory.markFormatSupportsSubsetOfColumns("JSONLines");
    factory.markFormatSupportsSubsetOfColumns("NDJSON");
    factory.markFormatSupportsSubsetOfColumns("JSONStringsEachRow");
}

void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONEachRow", &JSONUtils::fileSegmentationEngineJSONEachRow);
    factory.registerFileSegmentationEngine("JSONStringsEachRow", &JSONUtils::fileSegmentationEngineJSONEachRow);
    factory.registerFileSegmentationEngine("JSONLines", &JSONUtils::fileSegmentationEngineJSONEachRow);
    factory.registerFileSegmentationEngine("NDJSON", &JSONUtils::fileSegmentationEngineJSONEachRow);
}

void registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONEachRow", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONStringsEachRow", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONLines", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("NDJSON", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerJSONEachRowSchemaReader(FormatFactory & factory)
{
    auto register_schema_reader = [&](const String & format_name)
    {
        factory.registerSchemaReader(format_name, [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_unique<JSONEachRowSchemaReader>(buf, settings);
        });
        factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [](const FormatSettings & settings)
        {
            return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
        });
    };

    register_schema_reader("JSONEachRow");
    register_schema_reader("JSONLines");
    register_schema_reader("NDJSON");
    register_schema_reader("JSONStringsEachRow");
}

}
