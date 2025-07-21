#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <vector>

#include "SapFunctionCaller.h"
#include "utils.h"

SapFunctionCaller::SapFunctionCaller(SapRfcConnector* connector) : connector_(connector) {}

std::vector<std::map<std::string, std::string>> SapFunctionCaller::get_function_description(const std::string& function_name) {
    std::vector<std::map<std::string, std::string>> params;
    if (!connector_ || !connector_->con()) return params;
    std::u16string u_function_name = to_u16string(function_name);
    RFC_ERROR_INFO errorInfo;
    RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(connector_->con(), (SAP_UC*)u_function_name.c_str(), &errorInfo);
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
        param["optional"] = std::to_string(paramDesc.optional) == "1" ? "True" : "False";
        param["parameter_text"] = fromSAPUC(paramDesc.parameterText);
        param["default_value"] = fromSAPUC(paramDesc.defaultValue);
        params.push_back(param);
    }
    return params;
}

std::map<std::string, std::vector<std::map<std::string, std::string>>> SapFunctionCaller::call(
    const std::string& func,
    const std::map<std::string, std::string>& params,
    const std::map<std::string, std::vector<std::map<std::string, std::string>>>& tables
) {
    std::map<std::string, std::vector<std::map<std::string, std::string>>> result;
    if (!connector_ || !connector_->con()) return result;
    RFC_ERROR_INFO errorInfo;
    std::u16string u_func = to_u16string(func);
    RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(connector_->con(), (SAP_UC*)u_func.c_str(), &errorInfo);
    if (!funcDesc) return result;
    RFC_FUNCTION_HANDLE funcHandle = RfcCreateFunction(funcDesc, &errorInfo);
    if (!funcHandle) return result;
    // Set scalar parameters
    for (const auto& kv : params) {
        std::u16string u_name = to_u16string(kv.first);
        std::u16string u_value = to_u16string(kv.second);
        RfcSetChars(funcHandle, (SAP_UC*)u_name.c_str(), (SAP_UC*)u_value.c_str(), u_value.length(), &errorInfo);
    }
    // Set table parameters
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
    if (RfcInvoke(connector_->con(), funcHandle, &errorInfo) != RFC_OK) {
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

std::vector<std::map<std::string, std::string>> SapFunctionCaller::get_table_metadata(const std::string& table_name) {
    std::vector<std::map<std::string, std::string>> metadata;
    if (!connector_ || !connector_->con()) return metadata;
    RFC_ERROR_INFO errorInfo;
    std::u16string u_func = to_u16string("DDIF_FIELDINFO_GET");
    RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(connector_->con(), (SAP_UC*)u_func.c_str(), &errorInfo);
    if (funcDesc == nullptr) return metadata;
    RFC_FUNCTION_HANDLE func = RfcCreateFunction(funcDesc, &errorInfo);
    if (func == nullptr) return metadata;
    std::u16string u_tabname = to_u16string(table_name);
    RfcSetChars(func, (SAP_UC*)u"TABNAME", (SAP_UC*)u_tabname.c_str(), u_tabname.length(), &errorInfo);
    if (RfcInvoke(connector_->con(), func, &errorInfo) != RFC_OK) {
        RfcDestroyFunction(func, nullptr);
        return metadata;
    }
    RFC_TABLE_HANDLE fieldsTable;
    RfcGetTable(func, (SAP_UC*)u"DFIES_TAB", &fieldsTable, &errorInfo);
    unsigned int rowCount = 0;
    RfcGetRowCount(fieldsTable, &rowCount, &errorInfo);
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

std::map<std::string, std::vector<std::map<std::string, std::string>>> SapFunctionCaller::rfc_read_table_query(
    const std::string& sql,
    const std::string& sep,
    const int rowskips,
    const int rowcount
) {
    SQLQueryParts parsed = SqlParser::parse_sql_select(sql);
    std::vector<std::map<std::string, std::string>> fields;
    for (const auto& col : parsed.columns) {
        fields.push_back({ {"FIELDNAME", col} });
    }
    std::vector<std::map<std::string, std::string>> options;
    if (!parsed.whereClause.empty()) {
        std::string clause = parsed.whereClause;
        std::regex eq_no_space(R"(\s*=\s*)");
        clause = std::regex_replace(clause, eq_no_space, " = ");
        std::regex value_regex(R"(= (\w+))");
        clause = std::regex_replace(clause, value_regex, "= '$1'");
        options.push_back({ {"TEXT", clause} });
    }
    std::map<std::string, std::string> flatParams = {
        {"QUERY_TABLE", parsed.tableName}
    };
    if (!sep.empty()) {
        flatParams["DELIMITER"] = sep;
    }
    if (rowskips != 0) {
        flatParams["ROWSKIPS"] = std::to_string(rowskips);
    }
    if (rowcount != 0) {
        flatParams["ROWCOUNT"] = std::to_string(rowcount);
    }
    auto rows = call("/SAPDS/RFC_READ_TABLE", flatParams, {
        {"FIELDS", fields},
        {"OPTIONS", options}
    });
    return rows;
}

std::map<std::string, std::vector<std::map<std::string, std::string>>> SapFunctionCaller::query(
    const std::string& sql,
    const std::string& sep
) {
    SQLQueryParts parsed = SqlParser::parse_sql_select(sql);
    std::string tableName = parsed.tableName;
    std::vector<std::string> columns = parsed.columns;
    std::string whereClause = parsed.whereClause;
    std::vector<std::map<std::string, std::string>> fields;
    for (const auto& col : columns) {
        fields.push_back({ {"FIELDNAME", col} });
    }
    std::vector<std::map<std::string, std::string>> options;
    if (!whereClause.empty()) {
        std::string clause = whereClause;
        std::regex eq_no_space(R"(\s*=\s*)");
        clause = std::regex_replace(clause, eq_no_space, " = ");
        std::regex value_regex(R"(= (\w+))");
        clause = std::regex_replace(clause, value_regex, "= '$1'");
        options.push_back({ {"TEXT", clause} });
    }
    int rowcount = 0;
    int rowskips = 0;
    std::smatch match;
    std::regex limit_regex(R"(LIMIT (\d+))", std::regex_constants::icase);
    if (std::regex_search(sql, match, limit_regex)) {
        rowcount = std::stoi(match[1]);
    }
    std::regex offset_regex(R"(OFFSET (\d+))", std::regex_constants::icase);
    if (std::regex_search(sql, match, offset_regex)) {
        rowskips = std::stoi(match[1]);
    }
    std::map<std::string, std::string> flatParams = {
        {"QUERY_TABLE", tableName}
    };
    if (!sep.empty()) {
        flatParams["DELIMITER"] = sep;
    }
    if (rowskips != 0) {
        flatParams["ROWSKIPS"] = std::to_string(rowskips);
    }
    if (rowcount != 0) {
        flatParams["ROWCOUNT"] = std::to_string(rowcount);
    }
    auto result = call("RFC_READ_TABLE", flatParams, {
        {"FIELDS", fields},
        {"OPTIONS", options}
    });
    return result;
} 