#pragma once

#include <Poco/Logger.h>

namespace local_engine
{
class Logger
{
public:
    static void initConsoleLogger(const std::string & level);
    static void initFileLogger(const std::string & path, const std::string & level);
};
}


