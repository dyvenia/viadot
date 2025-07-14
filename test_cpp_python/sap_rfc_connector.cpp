#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <codecvt>
#include <locale>
#include <chrono>
#include <sstream>
#include <algorithm>
#include <variant>
#include <regex>

// Include SAP NetWeaver RFC SDK headers (assumed available)
#include "sapnwrfc.h"

std::u16string to_u16string(const std::string& utf8_str) {
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
    return converter.from_bytes(utf8_str);
}

std::string fromSAPUC(const SAP_UC* uc_str) {
    if (!uc_str) return "";

    std::u16string u16str(uc_str); // we assume it is null-terminated
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> convert;
    return convert.to_bytes(u16str);
}

std::string rfcDirectionToString(RFC_DIRECTION direction) {
    switch (direction) {
        case RFC_IMPORT: return "IMPORT";
        case RFC_EXPORT: return "EXPORT";
        case RFC_CHANGING: return "CHANGING";
        case RFC_TABLES: return "TABLES";
        default: return "UNKNOWN";
    }
}

std::string rfcTypeToString(RFCTYPE type) {
    switch (type) {
        case RFCTYPE_CHAR: return "CHAR";
        case RFCTYPE_DATE: return "DATE";
        case RFCTYPE_BCD: return "BCD";
        case RFCTYPE_TIME: return "TIME";
        case RFCTYPE_BYTE: return "BYTE";
        case RFCTYPE_TABLE: return "TABLE";
        case RFCTYPE_NUM: return "NUM";
        case RFCTYPE_FLOAT: return "FLOAT";
        case RFCTYPE_INT: return "INT";
        case RFCTYPE_INT2: return "INT2";
        case RFCTYPE_INT1: return "INT1";
        case RFCTYPE_STRING: return "STRING";
        case RFCTYPE_STRUCTURE: return "STRUCTURE";
        case RFCTYPE_XSTRING: return "XSTRING";
        default: return "UNKNOWN";
    }
}

struct SQLQueryParts {
    std::string tableName;
    std::vector<std::string> columns;
    std::string whereClause;
};

using ParamTable = std::vector<std::map<std::string, std::string>>;
using ParamValue = std::variant<std::string, ParamTable>;

class SapRfcConnector {
public:
    SapRfcConnector() : connected(false), connection(nullptr) {}

    bool connect(const std::string& user, const std::string& passwd, const std::string& ashost, const std::string& sysnr) {
        // Validate input parameters
        if (user.empty() || passwd.empty() || ashost.empty() || sysnr.empty()) {
            std::cerr << "Error: All connection parameters must be non-empty" << std::endl;
            return false;
        }

        RFC_CONNECTION_PARAMETER params[4];
        std::u16string u_user   = to_u16string(user);
        std::u16string u_passwd = to_u16string(passwd);
        std::u16string u_ashost = to_u16string(ashost);
        std::u16string u_sysnr  = to_u16string(sysnr);

        params[0].name  = cU("user");
        params[0].value = (SAP_UC*)u_user.c_str();
        params[1].name  = cU("passwd");
        params[1].value = (SAP_UC*)u_passwd.c_str();
        params[2].name  = cU("ashost");
        params[2].value = (SAP_UC*)u_ashost.c_str();
        params[3].name  = cU("sysnr");
        params[3].value = (SAP_UC*)u_sysnr.c_str();

        RFC_ERROR_INFO errorInfo;
        connection = RfcOpenConnection(params, 4, &errorInfo);
        
        if (!connection) {
            print_error(errorInfo);
            return false;
        }
        
        connected = true;
        std::cout << "Successfully connected to SAP" << std::endl;
        return true;
    }

    // error printer
    void print_error(RFC_ERROR_INFO errorInfo) {
        if (errorInfo.code != RFC_OK) {
            std::cerr << "RfcOpenConnection failed: " << fromSAPUC(errorInfo.message) << std::endl;
            std::cerr << "Error code: " << errorInfo.code << std::endl;
            std::cerr << "SAP error group: " << errorInfo.group << std::endl;
            std::cerr << "SAP error key: " << fromSAPUC(errorInfo.key) << std::endl;
            std::cerr << "SAP error message: " << fromSAPUC(errorInfo.message) << std::endl;
        }
    }

    // Getter for the connection handle (for internal use)
    RFC_CONNECTION_HANDLE con() const {
        return connection;
    }

    // Ping the SAP system to check connection
    bool check_connection() {
        if (!connected || !connection) return false;
        RFC_ERROR_INFO errorInfo;
        RFC_RC rc = RfcPing(connection, &errorInfo);
        return rc == RFC_OK;
    }

    // Close the SAP connection
    void close_connection() {
        if (connected && connection) {
            RFC_ERROR_INFO errorInfo;
            RfcCloseConnection(connection, &errorInfo);
            connected = false;
            connection = nullptr;
        }
    }

    // Get function description (metadata)
    std::vector<std::map<std::string, std::string>> get_function_description(const std::string& function_name) {
        std::vector<std::map<std::string, std::string>> params;
        if (!connected || !connection) return params;
        std::u16string u_function_name = to_u16string(function_name);
        RFC_ERROR_INFO errorInfo;
        RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(connection, (SAP_UC*)u_function_name.c_str(), &errorInfo);
        
        if (!funcDesc) return params;
        
        unsigned paramCount = 0;
        RfcGetParameterCount(funcDesc, &paramCount, &errorInfo);

        for (unsigned i = 0; i < paramCount; ++i) {
            RFC_PARAMETER_DESC paramDesc;
            RfcGetParameterDescByIndex(funcDesc, i, &paramDesc, &errorInfo);
            std::map<std::string, std::string> param;
            param["name"] = fromSAPUC(paramDesc.name);
            param["parameter_type"] = rfcTypeToString(paramDesc.type);
            param["direction"] = rfcDirectionToString(paramDesc.direction);
            param["nucLength"] = std::to_string(paramDesc.nucLength);
            param["ucLength"] = std::to_string(paramDesc.ucLength);
            param["optional"] = std::to_string(paramDesc.optional);
            param["parameter_text"] = fromSAPUC(paramDesc.parameterText);
            param["default_value"] = fromSAPUC(paramDesc.defaultValue);
            params.push_back(param);
        }

        return params;
    }



    std::map<std::string, std::vector<std::map<std::string, std::string>>> call(
        const std::string& func,
        const std::map<std::string, std::string>& params,
        const std::map<std::string, std::vector<std::map<std::string, std::string>>>& tables = {}
    ) {
        std::map<std::string, std::vector<std::map<std::string, std::string>>> result;

        if (!connected || !connection) return result;

        RFC_ERROR_INFO errorInfo;
        std::u16string u_func = to_u16string(func);
        RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(connection, (SAP_UC*)u_func.c_str(), &errorInfo);
        if (!funcDesc) return result;

        RFC_FUNCTION_HANDLE funcHandle = RfcCreateFunction(funcDesc, &errorInfo);
        if (!funcHandle) return result;

        // Set scalar parameters
        for (const auto& kv : params) {
            std::u16string u_name = to_u16string(kv.first);
            std::u16string u_value = to_u16string(kv.second);
            RfcSetChars(funcHandle, (SAP_UC*)u_name.c_str(), (SAP_UC*)u_value.c_str(), u_value.length(), &errorInfo);
        }

        // Set table parameters (e.g. FIELDS, OPTIONS)
        for (const auto& [tableName, rows] : tables) {
            std::u16string u_tableName = to_u16string(tableName);
            RFC_TABLE_HANDLE tableHandle = nullptr;
            RfcGetTable(funcHandle, (SAP_UC*)u_tableName.c_str(), &tableHandle, &errorInfo);
            if (!tableHandle) continue;

            RFC_TYPE_DESC_HANDLE rowDesc = RfcGetRowType(tableHandle, &errorInfo);

            for (const auto& row : rows) {
                RFC_STRUCTURE_HANDLE structHandle = RfcAppendNewRow(tableHandle, &errorInfo);
                for (const auto& [fieldName, fieldValue] : row) {
                    std::u16string u_fieldName = to_u16string(fieldName);
                    std::u16string u_fieldValue = to_u16string(fieldValue);
                    RfcSetChars(structHandle, (SAP_UC*)u_fieldName.c_str(), (SAP_UC*)u_fieldValue.c_str(), u_fieldValue.length(), &errorInfo);
                }
            }
        }

        // RFC call
        if (RfcInvoke(connection, funcHandle, &errorInfo) != RFC_OK) {
            RfcDestroyFunction(funcHandle, nullptr);
            return result;
        }

        // Helper to trim trailing spaces
        auto rtrim = [](const std::string& s) {
            auto end = s.find_last_not_of(" \t\r\n");
            return (end == std::string::npos) ? "" : s.substr(0, end + 1);
        };

        // Process all output tables
        unsigned paramCount = 0;
        RfcGetParameterCount(funcDesc, &paramCount, &errorInfo);

        for (unsigned i = 0; i < paramCount; ++i) {
            RFC_PARAMETER_DESC paramDesc;
            RfcGetParameterDescByIndex(funcDesc, i, &paramDesc, &errorInfo);

            if (paramDesc.type != RFCTYPE_TABLE) continue;

            RFC_TABLE_HANDLE tableHandle = nullptr;
            RfcGetTable(funcHandle, paramDesc.name, &tableHandle, &errorInfo);
            if (!tableHandle) continue;

            RFC_TYPE_DESC_HANDLE rowDescHandle = RfcGetRowType(tableHandle, &errorInfo);
            unsigned rowCount = 0;
            RfcGetRowCount(tableHandle, &rowCount, &errorInfo);
            unsigned fieldCount = 0;
            RfcGetFieldCount(rowDescHandle, &fieldCount, &errorInfo);

            std::string tableName = fromSAPUC(paramDesc.name);
            std::vector<std::map<std::string, std::string>> tableRows;

            for (unsigned rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
                RfcMoveTo(tableHandle, rowIdx, &errorInfo);
                RFC_STRUCTURE_HANDLE row = RfcGetCurrentRow(tableHandle, &errorInfo);
                std::map<std::string, std::string> rowMap;

                for (unsigned f = 0; f < fieldCount; ++f) {
                    RFC_FIELD_DESC fieldDesc;
                    RfcGetFieldDescByIndex(rowDescHandle, f, &fieldDesc, &errorInfo);

                    std::string fieldName = fromSAPUC(fieldDesc.name);
                    SAP_UC buffer[256] = {0};
                    RfcGetChars(row, fieldDesc.name, buffer, sizeof(buffer) / sizeof(SAP_UC) - 1, &errorInfo);
                    rowMap[fieldName] = rtrim(fromSAPUC(buffer));
                }

                tableRows.push_back(rowMap);
            }

            result[tableName] = tableRows;
        }

        RfcDestroyFunction(funcHandle, nullptr);
        return result;
    }

    std::vector<std::map<std::string, std::string>> get_table_metadata(const std::string& table_name) {
        std::vector<std::map<std::string, std::string>> metadata;
        if (!connected || !connection) return metadata;

        RFC_ERROR_INFO errorInfo;
        std::u16string u_func = to_u16string("DDIF_FIELDINFO_GET");
        RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(connection, (SAP_UC*)u_func.c_str(), &errorInfo);

        if (funcDesc == nullptr) return metadata;

        RFC_FUNCTION_HANDLE func = RfcCreateFunction(funcDesc, &errorInfo);
        if (func == nullptr) return metadata;

        // Set import parameters
        std::u16string u_tabname = to_u16string(table_name);
        std::cout << "u_tabname: " << fromSAPUC(u_tabname.c_str()) << std::endl;
        RfcSetChars(func, (SAP_UC*)u"TABNAME", (SAP_UC*)u_tabname.c_str(), u_tabname.length(), &errorInfo);


        // Call the function
        if (RfcInvoke(connection, func, &errorInfo) != RFC_OK) {
            RfcDestroyFunction(func, nullptr);
            return metadata;
        }

        // Get the table parameter
        RFC_TABLE_HANDLE fieldsTable;
        RfcGetTable(func, (SAP_UC*)u"DFIES_TAB", &fieldsTable, &errorInfo);

        // Get row count
        unsigned int rowCount = 0;
        RfcGetRowCount(fieldsTable, &rowCount, &errorInfo);

        //Trim if length is greater than xx
        auto rtrim = [](const std::string& s) {
            return s.substr(0, s.find_last_not_of(" \t\r\n") + 1);
        };

        for (unsigned int i = 0; i < rowCount; ++i) {
            RfcMoveTo(fieldsTable, i, nullptr);
            RFC_STRUCTURE_HANDLE row = RfcGetCurrentRow(fieldsTable, nullptr);

            SAP_UC fieldname[31] = {0};
            SAP_UC fieldtext[61] = {0};
            SAP_UC datatype[5] = {0};
            SAP_UC leng[7] = {0};

            RfcGetChars(row, (SAP_UC*)u"FIELDNAME", fieldname, 30, nullptr);
            RfcGetChars(row, (SAP_UC*)u"FIELDTEXT", fieldtext, 60, nullptr);
            RfcGetChars(row, (SAP_UC*)u"DATATYPE", datatype, 4, nullptr);
            RfcGetChars(row, (SAP_UC*)u"LENG", leng, 6, nullptr);

            std::map<std::string, std::string> field;
            field["FIELDNAME"] = rtrim(fromSAPUC(fieldname));
            field["FIELDTEXT"] = rtrim(fromSAPUC(fieldtext));
            field["DATATYPE"] = rtrim(fromSAPUC(datatype));
            field["LENG"] = rtrim(fromSAPUC(leng));

            metadata.push_back(field);
        }

        RfcDestroyFunction(func, nullptr);
        return metadata;
    }

    SQLQueryParts parse_sql_select(const std::string& sql) {
        SQLQueryParts parts;
        std::string upper_sql = sql;
        std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);

        auto select_pos = upper_sql.find("SELECT ");
        auto from_pos = upper_sql.find(" FROM ");
        auto where_pos = upper_sql.find(" WHERE ");

        if (select_pos == std::string::npos || from_pos == std::string::npos)
            throw std::invalid_argument("Invalid SQL SELECT syntax");

        std::string cols_str = sql.substr(select_pos + 7, from_pos - (select_pos + 7));
        std::string table_str;
        if (where_pos != std::string::npos)
            table_str = sql.substr(from_pos + 6, where_pos - (from_pos + 6));
        else
            table_str = sql.substr(from_pos + 6);

        parts.tableName = std::string(table_str);
        std::istringstream col_stream(cols_str);
        std::string col;
        while (std::getline(col_stream, col, ',')) {
            col.erase(std::remove_if(col.begin(), col.end(), ::isspace), col.end());
            parts.columns.push_back(col);
        }

        if (where_pos != std::string::npos)
            parts.whereClause = sql.substr(where_pos + 7);

        return parts;
    }

    std::map<std::string, std::vector<std::map<std::string, std::string>>> rfc_read_table_query(
        const std::string& sql,
        const std::string& sep = "|"
    ) {
        SQLQueryParts parsed = parse_sql_select(sql);

        // 1. FIELDS: list of columns
        std::vector<std::map<std::string, std::string>> fields;
        for (const auto& col : parsed.columns) {
            fields.push_back({ {"FIELDNAME", col} });
        }

        // 2. OPTIONS: WHERE filter TODO
        std::vector<std::map<std::string, std::string>> options;
        std::cout << "parsed.whereClause: " << parsed.whereClause << std::endl;
        if (!parsed.whereClause.empty()) {
            // Add spaces around "="
            std::string clause = parsed.whereClause;
            std::regex eq_no_space(R"(\s*=\s*)");
            clause = std::regex_replace(clause, eq_no_space, " = ");

            std::regex value_regex(R"(= (\w+))");
            clause = std::regex_replace(clause, value_regex, "= '$1'");

            // Surround the whole clause with double quotes
            // clause = "\"" + clause + "\"";

            std::cout << "DEBUG: final WHERE clause: " << clause << std::endl;
            options.push_back({ {"TEXT", clause} });
            std::cout << "Final TEXT value: [" << clause << "]\n";
        }

        // 3. Call parameters
        std::map<std::string, std::string> flatParams = {
            {"QUERY_TABLE", parsed.tableName},
            {"DELIMITER", sep}
        };
        
        // 4. RFC call
        auto rows = call("RFC_READ_TABLE", flatParams, {
            {"FIELDS", fields},
            {"OPTIONS", options}
        });

        return rows;
    }

    std::map<std::string, unsigned> get_library_info() {
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



    ~SapRfcConnector() {
        close_connection();
    }

private:
    bool connected;
    RFC_CONNECTION_HANDLE connection;
};

namespace py = pybind11;

PYBIND11_MODULE(sap_rfc_connector, m) {
    py::class_<SapRfcConnector>(m, "SapRfcConnector")
        .def(py::init<>())
        .def("connect", &SapRfcConnector::connect, py::arg("user"), py::arg("passwd"), py::arg("ashost"), py::arg("sysnr"), "Connect to SAP using SAP NetWeaver RFC SDK")
        .def("con", &SapRfcConnector::con, "Get the RFC connection handle (internal use)")
        .def("check_connection", &SapRfcConnector::check_connection, "Ping the SAP system to check connection")
        .def("close_connection", &SapRfcConnector::close_connection, "Close the SAP connection")
        .def("get_function_description", &SapRfcConnector::get_function_description, py::arg("function_name"), "Get function metadata from SAP")
        .def("call", &SapRfcConnector::call, py::arg("func"), py::arg("params"), py::arg("tables"), "Call a function with parameters and return results as a map")
        .def("get_table_metadata", &SapRfcConnector::get_table_metadata, py::arg("table_name"), "Get table metadata from SAP")
        .def("rfc_read_table_query", &SapRfcConnector::rfc_read_table_query, py::arg("sql"), py::arg("sep"), "RFC_READ_TABLE query")
        .def("get_library_info", &SapRfcConnector::get_library_info, "Get library information from SAP");
} 