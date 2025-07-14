#pragma once
#include <string>
#include <map>
#include <vector>

class MockSapRfcConnector {
public:
    MockSapRfcConnector() : connected(false) {}

    bool connect(const std::string&, const std::string&, const std::string&, const std::string&, const std::string&) {
        connected = true;
        return connected;
    }

    int con() const {
        return connected ? 42 : 0;
    }

    bool check_connection() {
        return connected;
    }

    void close_connection() {
        connected = false;
    }

    std::vector<std::map<std::string, std::string>> get_function_description(const std::string& function_name) {
        std::vector<std::map<std::string, std::string>> params;
        if (!connected) return params;
        if (function_name == "RFC_READ_TABLE") {
            params.push_back({{"name", "QUERY_TABLE"}, {"type", "CHAR"}, {"direction", "IMPORT"}});
            params.push_back({{"name", "FIELDS"}, {"type", "TABLE"}, {"direction", "IMPORT"}});
            params.push_back({{"name", "DATA"}, {"type", "TABLE"}, {"direction", "EXPORT"}});
        }
        return params;
    }

    std::map<std::string, std::string> call(const std::string& func, const std::map<std::string, std::string>&) {
        std::map<std::string, std::string> result;
        if (!connected) return result;
        if (func == "RFC_PING") {
            result["status"] = "pong";
        } else if (func == "RFC_READ_TABLE") {
            result["DATA"] = "mocked data";
        }
        return result;
    }

    std::vector<std::map<std::string, std::string>> get_table_metadata(const std::string& table_name) {
        std::vector<std::map<std::string, std::string>> metadata;
        if (!connected) return metadata;
        if (table_name == "SFLIGHT") {
            metadata.push_back({{"FIELDNAME", "CARRID"}, {"FIELDTEXT", "Carrier ID"}, {"DATATYPE", "CHAR"}, {"LENG", "3"}});
            metadata.push_back({{"FIELDNAME", "CONNID"}, {"FIELDTEXT", "Connection ID"}, {"DATATYPE", "NUMC"}, {"LENG", "4"}});
            metadata.push_back({{"FIELDNAME", "FLDATE"}, {"FIELDTEXT", "Flight Date"}, {"DATATYPE", "DATS"}, {"LENG", "8"}});
        }
        return metadata;
    }

private:
    bool connected;
}; 