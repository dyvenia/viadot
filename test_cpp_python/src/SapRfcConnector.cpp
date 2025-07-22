#include "SapRfcConnector.h"

#include <iostream>

#include "utils.h"

SapRfcConnector::SapRfcConnector() : connected(false), connection(nullptr) {}

SapRfcConnector::~SapRfcConnector() { close_connection(); }

bool SapRfcConnector::connect(const std::string &user,
                              const std::string &passwd,
                              const std::string &ashost,
                              const std::string &sysnr) {
    if (user.empty() || passwd.empty() || ashost.empty() || sysnr.empty()) {
        std::cerr << "Error: All connection parameters must be non-empty"
                  << std::endl;
        return false;
    }

    RFC_CONNECTION_PARAMETER params[4];
    // Assume utils::to_u16string is available as a utility function
    std::u16string u_user = utils::to_u16string(user);
    std::u16string u_passwd = utils::to_u16string(passwd);
    std::u16string u_ashost = utils::to_u16string(ashost);
    std::u16string u_sysnr = utils::to_u16string(sysnr);

    params[0].name = cU("user");
    params[0].value = (SAP_UC *)u_user.c_str();
    params[1].name = cU("passwd");
    params[1].value = (SAP_UC *)u_passwd.c_str();
    params[2].name = cU("ashost");
    params[2].value = (SAP_UC *)u_ashost.c_str();
    params[3].name = cU("sysnr");
    params[3].value = (SAP_UC *)u_sysnr.c_str();

    RFC_ERROR_INFO errorInfo;
    connection = RfcOpenConnection(params, 4, &errorInfo);

    if (!connection) {
        errorHandler.print_error(errorInfo);
        return false;
    }

    connected = true;

    return true;
}

void SapRfcConnector::close_connection() {
    if (connected && connection) {
        RFC_ERROR_INFO errorInfo;
        RfcCloseConnection(connection, &errorInfo);
        connected = false;
        connection = nullptr;
    }
}

bool SapRfcConnector::check_connection() {
    if (!connected || !connection) return false;

    RFC_ERROR_INFO errorInfo;
    RFC_RC rc = RfcPing(connection, &errorInfo);

    return rc == RFC_OK;
}

RFC_CONNECTION_HANDLE SapRfcConnector::con() const { return connection; }

std::map<std::string, unsigned> SapRfcConnector::get_library_info() {
    std::map<std::string, unsigned> info;
    unsigned majorVersion;
    unsigned minorVersion;
    unsigned patchLevel;
    RfcGetVersion(&majorVersion, &minorVersion, &patchLevel);
    info["majorVersion"] = majorVersion;
    info["minorVersion"] = minorVersion;
    info["patchLevel"] = patchLevel;

    return info;
}