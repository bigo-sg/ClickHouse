#include <Optimizer/Orca/CHToOrcaUtil.h>
#include "base/types.h"
namespace DB
{
namespace CHToOrcaUtil
{
String extracOrcaTypeName(DataTypePtr ch_type)
{
    const static std::map<String, String> ch_to_orca_type = {
        {"Int32", "int4"},
        {"Int64", "int8"},
        {"String", "text"}
    };
    String ch_type_name = ch_type->getName();
    // what about nullable , lowcardinary and other complex types ?
    auto it = ch_to_orca_type.find(ch_type_name);
    if (it == ch_to_orca_type.end())
        return "";
    return it->second;
}

Int32 extracOrcaTypeWidth(DataTypePtr ch_type)
{
    const static std::map<String, UInt32> ch_to_orca_type = {
        {"Int32", 4},
        {"Int64", 8},
        {"String", -1}
    };
    String ch_type_name = ch_type->getName();
    auto it = ch_to_orca_type.find(ch_type_name);
    if (it == ch_to_orca_type.end())
        return 0;
    return it->second;
}
}
}
