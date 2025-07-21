#pragma once

#include <string>
#include <map>

#include "sapnwrfc.h"
#include "SapErrorHandler.h"

class SapRfcConnector {
public:
    SapRfcConnector();
    ~SapRfcConnector();

    bool connect(const std::string& user, const std::string& passwd, const std::string& ashost, const std::string& sysnr);
    void close_connection();
    bool check_connection();
    RFC_CONNECTION_HANDLE con() const;
    std::map<std::string, unsigned> get_library_info();

private:
    bool connected;
    RFC_CONNECTION_HANDLE connection;
    SapErrorHandler errorHandler;
};
