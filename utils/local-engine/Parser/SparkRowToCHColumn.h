#pragma once

#include <memory>
#include <jni.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/CHColumnToSparkRow.h>
#include <base/StringRef.h>
#include <Common/JNIUtils.h>
#include <substrait/type.pb.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}
}

namespace local_engine
{
using namespace DB;
using namespace std;


struct SparkRowToCHColumnHelper
{
    DataTypes data_types;
    Block header;
    MutableColumns mutable_columns;

    SparkRowToCHColumnHelper(vector<string> & names, vector<string> & types)
        : data_types(names.size())
    {
        assert(names.size() == types.size());

        ColumnsWithTypeAndName columns(names.size());
        for (size_t i = 0; i < names.size(); ++i)
            columns[i] = std::move(ColumnWithTypeAndName(parseType(types[i]), names[i]));

        header = std::move(Block(columns));
        resetMutableColumns();
    }

    ~SparkRowToCHColumnHelper() = default;


    void resetMutableColumns()
    {
        mutable_columns = std::move(header.mutateColumns());
    }

    static DataTypePtr parseType(const string & type)
    {
        auto substrait_type = std::make_unique<substrait::Type>();
        auto ok = substrait_type->ParseFromString(type);
        if (!ok)
            throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::Type from string failed");
        return std::move(SerializedPlanParser::parseType(*substrait_type));
    }
};

class SparkRowToCHColumn
{
public:
    static jclass spark_row_interator_class;
    static jmethodID spark_row_interator_hasNext;
    static jmethodID spark_row_interator_next;

    // case 1: rows are batched (this is often directly converted from Block)
    static std::unique_ptr<Block> convertSparkRowInfoToCHColumn(const SparkRowInfo & spark_row_info, const Block & header);

    // case 2: provided with a sequence of spark UnsafeRow, convert them to a Block
    static Block *
    convertSparkRowItrToCHColumn(jobject java_iter, vector<string> & names, vector<string> & types)
    {
        SparkRowToCHColumnHelper helper(names, types);

        int attached;
        JNIEnv * env = JNIUtils::getENV(&attached);
        while (env->CallBooleanMethod(java_iter, spark_row_interator_hasNext))
        {
            jbyteArray row_data = static_cast<jbyteArray>(env->CallObjectMethod(java_iter, spark_row_interator_next));

            jsize len = env->GetArrayLength(row_data);
            char * c_arr = new char[len];
            env->GetByteArrayRegion(row_data, 0, len, reinterpret_cast<jbyte*>(c_arr));
            appendSparkRowToCHColumn(helper, c_arr, len);

            delete[] c_arr;
            c_arr = nullptr;
        }
        return getBlock(helper);
    }

    static void freeBlock(Block * block)
    {
        delete block;
        block = nullptr;
    }

private:
    static void appendSparkRowToCHColumn(SparkRowToCHColumnHelper & helper, char * buffer, int32_t length);
    static Block * getBlock(SparkRowToCHColumnHelper & helper);
};

class VariableLengthDataReader
{
public:
    explicit VariableLengthDataReader(const DataTypePtr& type_);
    virtual ~VariableLengthDataReader() = default;

    virtual Field read(char * buffer, size_t length);

private:
    virtual Field readDecimal(char * buffer, size_t length);
    virtual Field readString(char * buffer, size_t length);
    virtual Field readArray(char * buffer, size_t length);
    virtual Field readMap(char * buffer, size_t length);
    virtual Field readStruct(char * buffer, size_t length);

    const DataTypePtr & type;
    const DataTypePtr type_without_nullable;
    const WhichDataType which;
};

class FixedLengthDataReader
{
public:
    explicit FixedLengthDataReader(const DB::DataTypePtr & type_);
    virtual ~FixedLengthDataReader() = default;

    virtual Field read(char * buffer);

private:
    const DB::DataTypePtr & type;
    const DB::DataTypePtr type_without_nullable;
    const DB::WhichDataType which;
    
};
class SparkRowReader
{
public:
    explicit SparkRowReader(int32_t num_fields_, const DataTypes & field_types_)
        : num_fields(num_fields_), field_types(field_types_), bit_set_width_in_bytes(calculateBitSetWidthInBytes(num_fields))
    {
    }

    void assertIndexIsValid([[maybe_unused]] int index) const
    {
        assert(index >= 0);
        assert(index < num_fields);
    }

    bool isNullAt(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return isBitSet(buffer, ordinal);
    }

    const char* getRawDataForFixedNumber(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return reinterpret_cast<const char *>(getFieldOffset(ordinal));
    }

    int8_t getByte(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int8_t *>(getFieldOffset(ordinal));
    }

    uint8_t getUnsignedByte(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint8_t *>(getFieldOffset(ordinal));
    }


    int16_t getShort(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int16_t *>(getFieldOffset(ordinal));
    }

    uint16_t getUnsignedShort(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint16_t *>(getFieldOffset(ordinal));
    }

    int32_t getInt(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int32_t *>(getFieldOffset(ordinal));
    }

    uint32_t getUnsignedInt(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint32_t *>(getFieldOffset(ordinal));
    }

    int64_t getLong(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int64_t *>(getFieldOffset(ordinal));
    }

    float_t getFloat(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<float_t *>(getFieldOffset(ordinal));
    }

    double_t getDouble(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<double_t *>(getFieldOffset(ordinal));
    }

    StringRef getString(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        int64_t offset_and_size = getLong(ordinal);
        int32_t offset = static_cast<int32_t>(offset_and_size >> 32);
        int32_t size = static_cast<int32_t>(offset_and_size);
        return StringRef(reinterpret_cast<char *>(this->buffer + offset), size);
    }

    int32_t getStringSize(int ordinal) const
    {
        assertIndexIsValid(ordinal);
        return static_cast<int32_t>(getLong(ordinal));
    }

    void pointTo(char *buffer_, int32_t length_)
    {
        this->buffer = buffer_;
        this->length = length_;
    }

    Field getField(int ordinal) const
    {
        if (isNullAt(ordinal))
            return std::move(Null{});

        const auto & field_type = field_types[ordinal];
        if (BackingDataLengthCalculator::isFixedLengthDataType(removeNullable(field_type)))
        {
            FixedLengthDataReader reader(field_type);
            return std::move(reader.read(getFieldOffset(ordinal)));
        }
        else if (BackingDataLengthCalculator::isVariableLengthDataType(removeNullable(field_type)))
        {
            int64_t offset_and_size = 0;
            memcpy(&offset_and_size, buffer + bit_set_width_in_bytes + ordinal * 8, 8);
            const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
            const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);

            VariableLengthDataReader reader(field_type);
            return std::move(reader.read(buffer + offset, size));
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "SparkRowReader doesn't support type {}", field_type->getName());
    }


private:
    char * getFieldOffset(int ordinal) const { return buffer + bit_set_width_in_bytes + ordinal * 8L; }

    int32_t num_fields;
    const DataTypes field_types;

    char * buffer;
    int32_t length;
    int32_t bit_set_width_in_bytes;
};



}
