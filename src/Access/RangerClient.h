#pragma once

#include <base/types.h>

namespace DB
{

class RangerClient
{
public:
    class Params
    {
    public:
        String host;
        std::uint16_t port = 6080;

        String service_def;
        String service;

        String user;
        String password;
    };

    explicit RangerClient(const Params & params);
    ~RangerClient();

    RangerClient(const RangerClient &) = delete;
    RangerClient(RangerClient &&) = delete;
    RangerClient & operator=(const RangerClient &) = delete;
    RangerClient & operator=(RangerClient &&) = delete;

protected:
    const Params params;

    inline static const String REST_URL_SERVICE_DEFINITIONS{"/service/plugins/definitions/name/{}"};
    inline static const String REST_URL_SERVICE_POLICIES{"/plugins/policies/service/name/{}"};
};
}
