#include "Logger.h"
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/AsyncChannel.h>
#include <Poco/SimpleFileChannel.h>


using Poco::ConsoleChannel;
using Poco::AutoPtr;
using Poco::AsyncChannel;

void local_engine::Logger::initConsoleLogger(const std::string & level)
{
    AutoPtr<ConsoleChannel> chan(new ConsoleChannel);
    AutoPtr<AsyncChannel> async_chann(new AsyncChannel(chan));
    Poco::Logger::root().setChannel(async_chann);
    Poco::Logger::root().setLevel(level);
}

void local_engine::Logger::initFileLogger(const std::string & path, const std::string & level)
{
    Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::SimpleFileChannel>(new Poco::SimpleFileChannel(path)));
    Poco::Logger::root().setLevel(level);
}
