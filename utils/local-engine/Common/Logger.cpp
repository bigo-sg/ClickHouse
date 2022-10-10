#include "Logger.h"
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/AsyncChannel.h>


using Poco::ConsoleChannel;
using Poco::AutoPtr;
using Poco::AsyncChannel;

void local_engine::Logger::initConsoleLogger()
{
    AutoPtr<ConsoleChannel> p_cons(new ConsoleChannel);
    AutoPtr<AsyncChannel> p_async(new AsyncChannel(p_cons));
    Poco::Logger::root().setChannel(p_async);
    Poco::Logger::root().setLevel("debug");
    Poco::Logger::root().debug("init logger success");
}

