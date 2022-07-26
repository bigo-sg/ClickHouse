#pragma once
#include <DataTypes/IDataType.h>
#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{
namespace CHToOrcaUtil
{
String extracOrcaTypeName(DataTypePtr ch_type);
Int32 extracOrcaTypeWidth(DataTypePtr ch_type);
}
}
