#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

}

using namespace DB;
namespace local_engine
{

namespace
{
    class FunctionReinterpretAsStringSpark : public IFunction
    {
    public:
        static constexpr auto name = "reinterpretAsStringSpark";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReinterpretAsStringSpark>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override { return false; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            DataTypePtr from_type = arguments[0].type;
            if (!from_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as String because it is not contiguous in memory",
                    from_type->getName());

            DataTypePtr to_type = std::make_shared<DataTypeString>();
            return to_type;
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            auto from_type = arguments[0].type;

            ColumnPtr result;

            const IColumn & src = *arguments[0].column;
            MutableColumnPtr dst = result_type->createColumn();
            ColumnString * dst_concrete = assert_cast<ColumnString *>(dst.get());

            size_t rows = src.size();
            ColumnString::Chars & data_to = dst_concrete->getChars();
            ColumnString::Offsets & offsets_to = dst_concrete->getOffsets();
            offsets_to.resize(rows);

            ColumnString::Offset offset = 0;
            for (size_t i = 0; i < rows; ++i)
            {
                StringRef data = src.getDataAt(i);

                data_to.resize(offset + data.size + 1);
                memcpy(&data_to[offset], data.data, data.size);
                offset += data.size;
                data_to[offset] = 0;
                ++offset;
                offsets_to[i] = offset;
            }

            result = std::move(dst);
            return result;
        }
    };
}

void registerFunctionReinterpretAsStringSpark(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReinterpretAsStringSpark>();
}

}
