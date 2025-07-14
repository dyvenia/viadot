#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <vector>
#include <map>

class MockSapRfcConnector {
public:
    MockSapRfcConnector() : connected(false) {}

    bool connect(const std::string& user, const std::string& passwd, const std::string& ashost, const std::string& sysnr, const std::string& client) {
        connected = true;
        last_user = user;
        return connected;
    }

    // Simulate returning a fake connection handle (as int)
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
        } else {
            params.push_back({{"name", "DUMMY"}, {"type", "CHAR"}, {"direction", "IMPORT"}});
        }
        return params;
    }

    std::map<std::string, std::string> call(const std::string& func, const std::map<std::string, std::string>& params) {
        std::map<std::string, std::string> result;
        if (!connected) return result;
        if (func == "RFC_PING") {
            result["status"] = "pong";
        } else if (func == "RFC_READ_TABLE") {
            result["DATA"] = "mocked data";
        } else {
            result["status"] = "unknown function";
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
        } else {
            metadata.push_back({{"FIELDNAME", "ID"}, {"FIELDTEXT", "Generic ID"}, {"DATATYPE", "CHAR"}, {"LENG", "10"}});
        }
        return metadata;
    }

private:
    bool connected;
    std::string last_user;
};

namespace py = pybind11;

PYBIND11_MODULE(example, m) {
    py::class_<MockSapRfcConnector>(m, "MockSapRfcConnector")
        .def(py::init<>())
        .def("connect", &MockSapRfcConnector::connect, py::arg("user"), py::arg("passwd"), py::arg("ashost"), py::arg("sysnr"), py::arg("client"), "Mock connect to SAP")
        .def("con", &MockSapRfcConnector::con, "Get the mock connection handle")
        .def("check_connection", &MockSapRfcConnector::check_connection, "Mock ping the SAP system")
        .def("close_connection", &MockSapRfcConnector::close_connection, "Mock close the SAP connection")
        .def("get_function_description", &MockSapRfcConnector::get_function_description, py::arg("function_name"), "Get mock function metadata from SAP")
        .def("call", &MockSapRfcConnector::call, py::arg("func"), py::arg("params"), "Mock call a function with parameters and return results as a map")
        .def("get_table_metadata", &MockSapRfcConnector::get_table_metadata, py::arg("table_name"), "Get mock table metadata");
} 