#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <vector>

#include "SapFunctionCaller.h"
#include "utils.h"

// --- Helper Functions ---
namespace {
// Convert SAP_UC to std::string and trim trailing whitespace
inline std::string sap_uc_to_str(const SAP_UC *uc, size_t maxlen) {
  std::string s = fromSAPUC(uc);
  auto end = s.find_last_not_of(" \t\r\n");
  return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}
// Trim trailing whitespace from std::string
inline std::string rtrim(const std::string &s) {
  auto end = s.find_last_not_of(" \t\r\n");
  return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}
// Set scalar parameters for RFC function
void set_scalar_params(RFC_FUNCTION_HANDLE funcHandle,
                       const std::map<std::string, std::string> &params,
                       RFC_ERROR_INFO &errorInfo) {
  for (const auto &[key, value] : params) {
    std::u16string u_key = to_u16string(key);
    std::u16string u_value = to_u16string(value);
    RfcSetChars(funcHandle, (SAP_UC *)u_key.c_str(), (SAP_UC *)u_value.c_str(),
                u_value.length(), &errorInfo);
  }
}
// Set table parameters for RFC function
void set_table_params(
    RFC_FUNCTION_HANDLE funcHandle,
    const std::map<std::string, std::vector<std::map<std::string, std::string>>>
        &tables,
    RFC_ERROR_INFO &errorInfo) {
  for (const auto &[tableName, rows] : tables) {
    std::u16string u_tableName = to_u16string(tableName);
    RFC_TABLE_HANDLE tableHandle = nullptr;
    RfcGetTable(funcHandle, (SAP_UC *)u_tableName.c_str(), &tableHandle,
                &errorInfo);
    if (!tableHandle)
      continue;
    RFC_TYPE_DESC_HANDLE rowDesc = RfcGetRowType(tableHandle, &errorInfo);
    for (const auto &row : rows) {
      RFC_STRUCTURE_HANDLE structHandle =
          RfcAppendNewRow(tableHandle, &errorInfo);
      for (const auto &[fieldName, fieldValue] : row) {
        std::u16string u_fieldName = to_u16string(fieldName);
        std::u16string u_fieldValue = to_u16string(fieldValue);
        RfcSetChars(structHandle, (SAP_UC *)u_fieldName.c_str(),
                    (SAP_UC *)u_fieldValue.c_str(), u_fieldValue.length(),
                    &errorInfo);
      }
    }
  }
}
// Extract output tables from RFC function
std::map<std::string, std::vector<std::map<std::string, std::string>>>
extract_output_tables(RFC_FUNCTION_DESC_HANDLE funcDesc,
                      RFC_FUNCTION_HANDLE funcHandle,
                      RFC_ERROR_INFO &errorInfo) {
  std::map<std::string, std::vector<std::map<std::string, std::string>>> result;
  unsigned paramCount = 0;
  RfcGetParameterCount(funcDesc, &paramCount, &errorInfo);
  for (unsigned i = 0; i < paramCount; ++i) {
    RFC_PARAMETER_DESC paramDesc;
    RfcGetParameterDescByIndex(funcDesc, i, &paramDesc, &errorInfo);
    if (paramDesc.type != RFCTYPE_TABLE)
      continue;
    RFC_TABLE_HANDLE tableHandle = nullptr;
    RfcGetTable(funcHandle, paramDesc.name, &tableHandle, &errorInfo);
    if (!tableHandle)
      continue;
    RFC_TYPE_DESC_HANDLE rowDescHandle = RfcGetRowType(tableHandle, &errorInfo);
    unsigned rowCount = 0;
    RfcGetRowCount(tableHandle, &rowCount, &errorInfo);
    unsigned fieldCount = 0;
    RfcGetFieldCount(rowDescHandle, &fieldCount, &errorInfo);
    std::string tableName =
        sap_uc_to_str(paramDesc.name, sizeof(paramDesc.name) / sizeof(SAP_UC));
    std::vector<std::map<std::string, std::string>> tableRows;
    for (unsigned rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      RfcMoveTo(tableHandle, rowIdx, &errorInfo);
      RFC_STRUCTURE_HANDLE row = RfcGetCurrentRow(tableHandle, &errorInfo);
      std::map<std::string, std::string> rowMap;
      for (unsigned f = 0; f < fieldCount; ++f) {
        RFC_FIELD_DESC fieldDesc;
        RfcGetFieldDescByIndex(rowDescHandle, f, &fieldDesc, &errorInfo);
        std::string fieldName = sap_uc_to_str(
            fieldDesc.name, sizeof(fieldDesc.name) / sizeof(SAP_UC));
        SAP_UC buffer[256] = {0};
        RfcGetChars(row, fieldDesc.name, buffer,
                    sizeof(buffer) / sizeof(SAP_UC) - 1, &errorInfo);
        rowMap[fieldName] = rtrim(fromSAPUC(buffer));
      }
      tableRows.push_back(std::move(rowMap));
    }
    result[tableName] = std::move(tableRows);
  }
  return result;
}
} // namespace

// --- SapFunctionCaller Implementation ---

SapFunctionCaller::SapFunctionCaller(SapRfcConnector *connector)
    : connector_(connector) {}

std::vector<std::map<std::string, std::string>>
SapFunctionCaller::get_function_description(const std::string &function_name) {
  std::vector<std::map<std::string, std::string>> params;
  if (!connector_ || !connector_->con())
    return params;
  std::u16string u_function_name = to_u16string(function_name);
  RFC_ERROR_INFO errorInfo;
  RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(
      connector_->con(), (SAP_UC *)u_function_name.c_str(), &errorInfo);
  if (!funcDesc)
    return params;
  unsigned paramCount = 0;
  RfcGetParameterCount(funcDesc, &paramCount, &errorInfo);
  params.reserve(paramCount);
  for (unsigned i = 0; i < paramCount; ++i) {
    RFC_PARAMETER_DESC paramDesc;
    RfcGetParameterDescByIndex(funcDesc, i, &paramDesc, &errorInfo);
    std::map<std::string, std::string> param;
    param["name"] = fromSAPUC(paramDesc.name);
    param["parameter_type"] = rfcTypeToString(paramDesc.type);
    param["direction"] = rfcDirectionToString(paramDesc.direction);
    param["nucLength"] = std::to_string(paramDesc.nucLength);
    param["ucLength"] = std::to_string(paramDesc.ucLength);
    param["optional"] = (paramDesc.optional == 1) ? "True" : "False";
    param["parameter_text"] = fromSAPUC(paramDesc.parameterText);
    param["default_value"] = fromSAPUC(paramDesc.defaultValue);
    params.push_back(std::move(param));
  }
  return params;
}

std::map<std::string, std::vector<std::map<std::string, std::string>>>
SapFunctionCaller::call(
    const std::string &func, const std::map<std::string, std::string> &params,
    const std::map<std::string, std::vector<std::map<std::string, std::string>>>
        &tables) {
  std::map<std::string, std::vector<std::map<std::string, std::string>>> result;
  if (!connector_ || !connector_->con())
    return result;
  RFC_ERROR_INFO errorInfo;
  std::u16string u_func = to_u16string(func);
  RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(
      connector_->con(), (SAP_UC *)u_func.c_str(), &errorInfo);
  if (!funcDesc)
    return result;
  RFC_FUNCTION_HANDLE funcHandle = RfcCreateFunction(funcDesc, &errorInfo);
  if (!funcHandle)
    return result;
  set_scalar_params(funcHandle, params, errorInfo);
  set_table_params(funcHandle, tables, errorInfo);
  if (RfcInvoke(connector_->con(), funcHandle, &errorInfo) != RFC_OK) {
    RfcDestroyFunction(funcHandle, nullptr);
    return result;
  }
  result = extract_output_tables(funcDesc, funcHandle, errorInfo);
  RfcDestroyFunction(funcHandle, nullptr);
  return result;
}

std::vector<std::map<std::string, std::string>>
SapFunctionCaller::get_table_metadata(const std::string &table_name) {
  std::vector<std::map<std::string, std::string>> metadata;
  if (!connector_ || !connector_->con())
    return metadata;
  RFC_ERROR_INFO errorInfo;
  std::u16string u_func = to_u16string("DDIF_FIELDINFO_GET");
  RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(
      connector_->con(), (SAP_UC *)u_func.c_str(), &errorInfo);
  if (funcDesc == nullptr)
    return metadata;
  RFC_FUNCTION_HANDLE func = RfcCreateFunction(funcDesc, &errorInfo);
  if (func == nullptr)
    return metadata;
  std::u16string u_tabname = to_u16string(table_name);
  RfcSetChars(func, (SAP_UC *)u"TABNAME", (SAP_UC *)u_tabname.c_str(),
              u_tabname.length(), &errorInfo);
  if (RfcInvoke(connector_->con(), func, &errorInfo) != RFC_OK) {
    RfcDestroyFunction(func, nullptr);
    return metadata;
  }
  RFC_TABLE_HANDLE fieldsTable;
  RfcGetTable(func, (SAP_UC *)u"DFIES_TAB", &fieldsTable, &errorInfo);
  unsigned int rowCount = 0;
  RfcGetRowCount(fieldsTable, &rowCount, &errorInfo);
  for (unsigned int i = 0; i < rowCount; ++i) {
    RfcMoveTo(fieldsTable, i, nullptr);
    RFC_STRUCTURE_HANDLE row = RfcGetCurrentRow(fieldsTable, nullptr);
    SAP_UC fieldname[31] = {0};
    SAP_UC fieldtext[61] = {0};
    SAP_UC datatype[5] = {0};
    SAP_UC leng[7] = {0};
    RfcGetChars(row, (SAP_UC *)u"FIELDNAME", fieldname, 30, nullptr);
    RfcGetChars(row, (SAP_UC *)u"FIELDTEXT", fieldtext, 60, nullptr);
    RfcGetChars(row, (SAP_UC *)u"DATATYPE", datatype, 4, nullptr);
    RfcGetChars(row, (SAP_UC *)u"LENG", leng, 6, nullptr);
    std::map<std::string, std::string> field;
    field["FIELDNAME"] = rtrim(fromSAPUC(fieldname));
    field["FIELDTEXT"] = rtrim(fromSAPUC(fieldtext));
    field["DATATYPE"] = rtrim(fromSAPUC(datatype));
    field["LENG"] = rtrim(fromSAPUC(leng));
    metadata.push_back(std::move(field));
  }
  RfcDestroyFunction(func, nullptr);
  return metadata;
}

std::map<std::string, std::vector<std::map<std::string, std::string>>>
SapFunctionCaller::rfc_read_table_query(const std::string &sql,
                                        const std::string &sep,
                                        const int rowskips,
                                        const int rowcount) {
  SQLQueryParts parsed = SqlParser::parse_sql_select(sql);
  std::vector<std::map<std::string, std::string>> fields;
  fields.reserve(parsed.columns.size());
  for (const auto &col : parsed.columns) {
    fields.push_back({{"FIELDNAME", col}});
  }
  std::vector<std::map<std::string, std::string>> options;
  if (!parsed.whereClause.empty()) {
    std::string clause = parsed.whereClause;
    clause = std::regex_replace(clause, std::regex(R"(\s*=\s*)"), " = ");
    clause = std::regex_replace(clause, std::regex(R"(= (\w+))"), "= '$1'");
    options.push_back({{"TEXT", clause}});
  }
  std::map<std::string, std::string> flatParams = {
      {"QUERY_TABLE", parsed.tableName}};
  if (!sep.empty())
    flatParams["DELIMITER"] = sep;
  if (rowskips != 0)
    flatParams["ROWSKIPS"] = std::to_string(rowskips);
  if (rowcount != 0)
    flatParams["ROWCOUNT"] = std::to_string(rowcount);
  return call("/SAPDS/RFC_READ_TABLE", flatParams,
              {{"FIELDS", fields}, {"OPTIONS", options}});
}
