#pragma once

#include <Processors/ISimpleTransform.h>
#include "Common/StringUtils.h"
#include "Columns/IColumn.h"
#include "Core/Block.h"
#include "DataTypes/Serializations/ISerialization.h"

namespace local_engine
{
class PartitionColumnFillingTransform : public DB::ISimpleTransform
{
public:
    PartitionColumnFillingTransform(
        const DB::Block & input_,
        const DB::Block & output_,
        const PartitionValues & partition_columns_);
    void transform(DB::Chunk & chunk) override;
    String getName() const override
    {
        return "PartitionColumnFillingTransform";
    }

private:
    DB::ColumnPtr createPartitionColumn(const String & parition_col, const String & partition_col_value, size_t row);
    static DB::ColumnPtr tryWrapPartitionColumn(const DB::ColumnPtr & nested_col, DB::DataTypePtr original_data_type);

    PartitionValues partition_column_values;
    std::map<String, String> partition_columns;
};

}


