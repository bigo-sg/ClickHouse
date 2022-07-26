#pragma once

#include <boost/core/noncopyable.hpp>
#include <Optimizer/Stats/StatisticsData.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
namespace DB
{
/// Interface to access tables/columns statistics information
class StatisticsAccessor : public boost::noncopyable
{
public:
    static StatisticsAccessor & instance();

    /// Get table statistics data by query
    TableStatisticsList getStatistics(ContextPtr context, ASTPtr query);
protected:
    StatisticsAccessor();

    Poco::Logger * logger = &Poco::Logger::get("StatisticsAccessor");

    static TableStatisticsList genFakeData(ContextPtr context);
};
}
