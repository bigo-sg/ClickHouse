#include <cstdio>
#include <iomanip>
#include <iostream>
#include <Columns/ColumnsNumber.h>
#include <Common/Stopwatch.h>

using namespace DB;

namespace test
{

template <typename T>
static MutableColumnPtr createColumn(size_t n)
{
    auto column = ColumnVector<T>::create();
    auto & values = column->getData();

    for (size_t i = 0; i < n; ++i)
        values.push_back(static_cast<T>(i));

    return std::move(column);
}

static IColumn::Offsets createOffsets(size_t n)
{
    auto res = IColumn::Offsets(n);
    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < n; ++i)
    {
        res[i] = prev_offset + (std::rand() % 128 + 10);
        prev_offset = res[i];
    }
    return res;
}

template <typename T>
static void testSingleType(const String & msg, const IColumn::Offsets & offsets)
{
    auto column = createColumn<T>(1000000);

    double sum = 0.0;
    for (size_t i=0; i<10; i++)
    {
        Stopwatch watch;
        auto output = column->replicate(offsets);
        watch.stop();

        sum += watch.elapsedMilliseconds();
    }
    std::cerr << msg << " cost " << sum / 10 << " ms" << std::endl;
}

static void testAllTypes()
{
    IColumn::Offsets offsets(createOffsets(1000000));
    testSingleType<UInt8>("UInt8", offsets);
    testSingleType<Int8>("Int8", offsets);
    testSingleType<UInt16>("UInt16", offsets);
    testSingleType<Int16>("Int16", offsets);
    testSingleType<UInt32>("UInt32", offsets);
    testSingleType<Int32>("Int32", offsets);
    testSingleType<UInt64>("UInt64", offsets);
    testSingleType<Int64>("Int64", offsets);
}

}

int main()
{
    test::testAllTypes();
    return 0;
}
