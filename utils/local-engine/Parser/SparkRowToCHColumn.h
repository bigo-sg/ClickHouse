#pragma once

#include <memory>
#include <jni.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Parser/CHColumnToSparkRow.h>
#include <base/StringRef.h>
#include <Common/JNIUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
using namespace DB;
using namespace std;


struct SparkRowToCHColumnHelper
{
    SparkRowToCHColumnHelper(vector<string>& names, vector<string>& types, vector<bool>& isNullables)
    {
        internal_cols = std::make_unique<std::vector<ColumnWithTypeAndName>>();
        internal_cols->reserve(names.size());
        data_types = std::make_unique<std::vector<DataTypePtr>>();
        data_types->reserve(names.size());
        for (size_t i = 0; i < names.size(); ++i)
        {
            const auto & name = names[i];
            const auto & type = types[i];
            const bool is_nullable = isNullables[i];
            auto data_type = parseType(type, is_nullable);
            internal_cols->push_back(ColumnWithTypeAndName(data_type, name));
            data_types->push_back(data_type);
        }
        header = std::make_shared<Block>(*std::move(internal_cols));
        resetWrittenColumns();
    }

    unique_ptr<vector<ColumnWithTypeAndName>> internal_cols; //for headers
    unique_ptr<vector<MutableColumnPtr>> cols;
    unique_ptr<vector<DataTypePtr>> data_types;
    shared_ptr<Block> header;

    void resetWrittenColumns()
    {
        cols = make_unique<vector<MutableColumnPtr>>();
        for (auto & i : *internal_cols)
        {
            cols->push_back(i.type->createColumn());
        }
    }

    static DataTypePtr inline wrapNullableType(bool isNullable, DataTypePtr nested_type)
    {
        if (isNullable)
        {
            return std::make_shared<DataTypeNullable>(nested_type);
        }
        else
        {
            return nested_type;
        }
    }

    //parse Spark type name to CH DataType
    static DataTypePtr parseType(const string& type, const bool isNullable)
    {
        DataTypePtr internal_type = nullptr;
        auto & factory = DataTypeFactory::instance();
        if ("boolean" == type)
        {
            internal_type = factory.get("UInt8");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("byte" == type)
        {
            internal_type = factory.get("Int8");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("short" == type)
        {
            internal_type = factory.get("Int16");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("integer" == type)
        {
            internal_type = factory.get("Int32");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("long" == type)
        {
            internal_type = factory.get("Int64");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("string" == type)
        {
            internal_type = factory.get("String");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("float" == type)
        {
            internal_type = factory.get("Float32");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("double" == type)
        {
            internal_type = factory.get("Float64");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        else if ("date" == type)
        {
            internal_type = factory.get("Date32");
            internal_type = wrapNullableType(isNullable, internal_type);
        }
        /// TODO: Support for other types like decimal/map/struct/array
        else
        {
            throw Exception(0, "doesn't support spark type {}", type);
        }
        return internal_type;
    }
};

class SparkRowToCHColumn
{
public:
    static jclass spark_row_interator_class;
    static jmethodID spark_row_interator_hasNext;
    static jmethodID spark_row_interator_next;

    // case 1: rows are batched (this is often directly converted from Block)
    static std::unique_ptr<Block> convertSparkRowInfoToCHColumn(SparkRowInfo & spark_row_info, Block & header);

    // case 2: provided with a sequence of spark UnsafeRow, convert them to a Block
    static Block *
    convertSparkRowItrToCHColumn(jobject java_iter, vector<string> & names, vector<string> & types, vector<bool> & is_nullables)
    {
        SparkRowToCHColumnHelper helper(names, types, is_nullables);

        int attached;
        JNIEnv * env = JNIUtils::getENV(&attached);
        while(env->CallBooleanMethod(java_iter,spark_row_interator_hasNext)){
            jbyteArray row_data = static_cast<jbyteArray>(env->CallObjectMethod(java_iter, spark_row_interator_next));

            jsize len = env->GetArrayLength(row_data);
            char * c_arr = new char[len];
            env->GetByteArrayRegion(row_data, 0, len, reinterpret_cast<jbyte*>(c_arr));
            appendSparkRowToCHColumn(helper, c_arr, len);
            delete[] c_arr;
        }
        return getWrittenBlock(helper);
    }

    static void freeBlock(Block * block) { delete block; }

private:
    static void appendSparkRowToCHColumn(SparkRowToCHColumnHelper & helper, char * buffer , int32_t length);
    static Block * getWrittenBlock(SparkRowToCHColumnHelper & helper);
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
    const DB::WhichDataType which;
    
};
class SparkRowReader
{
public:
    explicit SparkRowReader(int32_t num_fields_, const DataTypes & field_types_)
        : num_fields(num_fields_), field_types(field_types_), bit_set_width_in_bytes(calculateBitSetWidthInBytes(num_fields))
    {
    }

    void assertIndexIsValid([[maybe_unused]] int index) 
    {
        assert(index >= 0);
        assert(index < num_fields);
    }

    bool isNullAt(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return isBitSet(buffer, ordinal);
    }

    char* getRawDataForFixedNumber(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return reinterpret_cast<char *>(getFieldOffset(ordinal));
    }

    int8_t getByte(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int8_t *>(getFieldOffset(ordinal));
    }

    uint8_t getUnsignedByte(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint8_t *>(getFieldOffset(ordinal));
    }


    int16_t getShort(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int16_t *>(getFieldOffset(ordinal));
    }

    uint16_t getUnsignedShort(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint16_t *>(getFieldOffset(ordinal));
    }

    int32_t getInt(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int32_t *>(getFieldOffset(ordinal));
    }

    uint32_t getUnsignedInt(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<uint32_t *>(getFieldOffset(ordinal));
    }

    int64_t getLong(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<int64_t *>(getFieldOffset(ordinal));
    }

    float_t getFloat(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<float_t *>(getFieldOffset(ordinal));
    }

    double_t getDouble(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return *reinterpret_cast<double_t *>(getFieldOffset(ordinal));
    }

    StringRef getString(int ordinal)
    {
        assertIndexIsValid(ordinal);
        int64_t offset_and_size = getLong(ordinal);
        int32_t offset = static_cast<int32_t>(offset_and_size >> 32);
        int32_t size = static_cast<int32_t>(offset_and_size);
        return StringRef(reinterpret_cast<char *>(this->buffer + offset), size);
    }

    int32_t getStringSize(int ordinal)
    {
        assertIndexIsValid(ordinal);
        return static_cast<int32_t>(getLong(ordinal));
    }

    void pointTo(char *buffer_, int32_t length_)
    {
        this->buffer = buffer_;
        this->length = length_;
    }

    Field getField(int ordinal)
    {
        if (isNullAt(ordinal))
            return std::move(Null{});

        const auto & field_type = field_types[ordinal];
        if (BackingDataLengthCalculator::isFixedLengthDataType(field_type))
        {
            FixedLengthDataReader reader(field_type);
            return std::move(reader.read(getFieldOffset(ordinal)));
        }
        else if (BackingDataLengthCalculator::isVariableLengthDataType(field_type))
        {
            int64_t offset_and_size = 0;
            memcpy(&offset_and_size, buffer + bit_set_width_in_bytes + ordinal * 8, 8);
            const int64_t offset = BackingDataLengthCalculator::extractOffset(offset_and_size);
            const int64_t size = BackingDataLengthCalculator::extractSize(offset_and_size);

            VariableLengthDataReader reader(field_type);
            /*
            if (WhichDataType(field_type).isMap())
            {
                std::cerr << "length:" << length << ",offset:" << offset << ",size:" << size << std::endl;
                std::abort();
            }
            */
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
