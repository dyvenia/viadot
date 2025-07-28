#include "SapFunctionCaller.h"

#include <iostream>  // only for debugging
#include <string>
#include <unordered_map>
#include <vector>

#include "utils.h"

namespace {

constexpr char *sap_metadata_table_name = "DFIES_TAB";
constexpr char *sap_metadata_function_name = "DDIF_FIELDINFO_GET";
constexpr char *sap_metadata_table_name_field = "TABNAME";
constexpr char *sap_metadata_datatype_field = "DATATYPE";
constexpr char *sap_metadata_leng_field = "LENG";
constexpr char *sap_metadata_name_field = "FIELDNAME";
constexpr char *sap_metadata_text_field = "FIELDTEXT";

constexpr int fieldname_length = 30;
constexpr int fieldtext_length = 60;
constexpr int datatype_length = 4;
constexpr int leng_length = 6;

// Set scalar parameters for RFC function
void set_scalar_params(RFC_FUNCTION_HANDLE funcHandle,
                       const params_type &params, RFC_ERROR_INFO &errorInfo) {
    for (const auto &[key, value] : params) {
        std::u16string u_key = utils::to_u16string(key);
        std::u16string u_value = utils::to_u16string(value);
        RfcSetChars(funcHandle, (SAP_UC *)u_key.c_str(),
                    (SAP_UC *)u_value.c_str(), u_value.length(), &errorInfo);
    }
}
// Set table parameters for RFC function
void set_table_params(RFC_FUNCTION_HANDLE funcHandle, const tables_type &tables,
                      RFC_ERROR_INFO &errorInfo) {
    for (const auto &[tableName, rows] : tables) {
        std::u16string u_tableName = utils::to_u16string(tableName);
        RFC_TABLE_HANDLE tableHandle = nullptr;
        RfcGetTable(funcHandle, (SAP_UC *)u_tableName.c_str(), &tableHandle,
                    &errorInfo);

        if (!tableHandle) continue;

        RFC_TYPE_DESC_HANDLE rowDesc = RfcGetRowType(tableHandle, &errorInfo);

        for (const auto &row : rows) {
            RFC_STRUCTURE_HANDLE structHandle =
                RfcAppendNewRow(tableHandle, &errorInfo);

            for (const auto &[fieldName, fieldValue] : row) {
                std::u16string u_fieldName = utils::to_u16string(fieldName);
                std::u16string u_fieldValue = utils::to_u16string(fieldValue);
                RfcSetChars(structHandle, (SAP_UC *)u_fieldName.c_str(),
                            (SAP_UC *)u_fieldValue.c_str(),
                            u_fieldValue.length(), &errorInfo);
            }
        }
    }
}

// Extract output tables from RFC function
tables_type extract_output_tables(RFC_FUNCTION_DESC_HANDLE funcDesc,
                                  RFC_FUNCTION_HANDLE funcHandle,
                                  RFC_ERROR_INFO &errorInfo) {
    tables_type result;
    unsigned paramCount = 0;
    RfcGetParameterCount(funcDesc, &paramCount, &errorInfo);

    for (unsigned i = 0; i < paramCount; ++i) {
        RFC_PARAMETER_DESC paramDesc;
        RfcGetParameterDescByIndex(funcDesc, i, &paramDesc, &errorInfo);

        if (paramDesc.type != RFCTYPE_TABLE) continue;

        RFC_TABLE_HANDLE tableHandle = nullptr;
        RfcGetTable(funcHandle, paramDesc.name, &tableHandle, &errorInfo);
        if (!tableHandle) continue;

        RFC_TYPE_DESC_HANDLE rowDescHandle =
            RfcGetRowType(tableHandle, &errorInfo);
        unsigned rowCount = 0;
        unsigned fieldCount = 0;
        RfcGetRowCount(tableHandle, &rowCount, &errorInfo);
        RfcGetFieldCount(rowDescHandle, &fieldCount, &errorInfo);

        const std::string tableName = utils::rtrim_sapstring(paramDesc.name);
        std::vector<std::unordered_map<std::string, std::string>> tableRows;

        for (unsigned rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
            RfcMoveTo(tableHandle, rowIdx, &errorInfo);
            RFC_STRUCTURE_HANDLE row =
                RfcGetCurrentRow(tableHandle, &errorInfo);
            std::unordered_map<std::string, std::string> rowMap;

            for (unsigned f = 0; f < fieldCount; ++f) {
                RFC_FIELD_DESC fieldDesc;
                RfcGetFieldDescByIndex(rowDescHandle, f, &fieldDesc,
                                       &errorInfo);
                const std::string fieldName =
                    utils::rtrim_sapstring(fieldDesc.name);
                SAP_UC buffer[256] = {0};
                RfcGetChars(row, fieldDesc.name, buffer,
                            sizeof(buffer) / sizeof(SAP_UC) - 1, &errorInfo);
                rowMap[fieldName] = utils::rtrim_sapstring(buffer);
            }
            tableRows.push_back(std::move(rowMap));
        }

        result[tableName] = std::move(tableRows);
    }

    return result;
}
}  // namespace

SapFunctionCaller::SapFunctionCaller(SapRfcConnector *connector)
    : connector_(connector) {}

metadata_type SapFunctionCaller::get_function_description(
    const std::string &function_name) {
    metadata_type params;

    if (!connector_ || !connector_->con()) return params;

    std::u16string u_function_name = utils::to_u16string(function_name);
    RFC_ERROR_INFO errorInfo;
    RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(
        connector_->con(), (SAP_UC *)u_function_name.c_str(), &errorInfo);

    if (!funcDesc) return params;

    unsigned paramCount = 0;
    RfcGetParameterCount(funcDesc, &paramCount, &errorInfo);
    params.reserve(paramCount);

    for (unsigned i = 0; i < paramCount; ++i) {
        RFC_PARAMETER_DESC paramDesc;
        RfcGetParameterDescByIndex(funcDesc, i, &paramDesc, &errorInfo);
        std::unordered_map<std::string, std::string> param;

        param["name"] = utils::fromSAPUC(paramDesc.name);
        param["parameter_type"] = utils::rfcTypeToString(paramDesc.type);
        param["direction"] = utils::rfcDirectionToString(paramDesc.direction);
        param["nucLength"] = std::to_string(paramDesc.nucLength);
        param["ucLength"] = std::to_string(paramDesc.ucLength);
        param["optional"] = (paramDesc.optional == 1) ? "True" : "False";
        param["parameter_text"] = utils::fromSAPUC(paramDesc.parameterText);
        param["default_value"] = utils::fromSAPUC(paramDesc.defaultValue);

        params.push_back(std::move(param));
    }

    return params;
}

tables_type SapFunctionCaller::call(const std::string &func,
                                    const params_type &params,
                                    const tables_type &tables) {
    tables_type result;

    if (!connector_ || !connector_->con()) return result;

    RFC_ERROR_INFO errorInfo;
    std::u16string u_func = utils::to_u16string(func);
    RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(
        connector_->con(), (SAP_UC *)u_func.c_str(), &errorInfo);

    if (!funcDesc) return result;

    RFC_FUNCTION_HANDLE funcHandle = RfcCreateFunction(funcDesc, &errorInfo);
    if (!funcHandle) return result;

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

metadata_type SapFunctionCaller::get_table_metadata(
    const std::string &table_name) {
    metadata_type metadata;

    if (!connector_ || !connector_->con()) return metadata;

    RFC_ERROR_INFO errorInfo;
    std::u16string u_func = utils::to_u16string(sap_metadata_function_name);
    RFC_FUNCTION_DESC_HANDLE funcDesc = RfcGetFunctionDesc(
        connector_->con(), (SAP_UC *)u_func.c_str(), &errorInfo);

    if (funcDesc == nullptr) return metadata;

    RFC_FUNCTION_HANDLE func = RfcCreateFunction(funcDesc, &errorInfo);

    if (func == nullptr) return metadata;

    std::u16string u_tabname = utils::to_u16string(table_name);
    RfcSetChars(func, (SAP_UC *)sap_metadata_table_name_field,
                (SAP_UC *)u_tabname.c_str(), u_tabname.length(), &errorInfo);

    if (RfcInvoke(connector_->con(), func, &errorInfo) != RFC_OK) {
        RfcDestroyFunction(func, nullptr);
        return metadata;
    }

    RFC_TABLE_HANDLE fieldsTable;
    RfcGetTable(func, (SAP_UC *)sap_metadata_table_name_field, &fieldsTable,
                &errorInfo);
    unsigned int rowCount = 0;
    RfcGetRowCount(fieldsTable, &rowCount, &errorInfo);

    for (unsigned int i = 0; i < rowCount; ++i) {
        RfcMoveTo(fieldsTable, i, nullptr);
        RFC_STRUCTURE_HANDLE row = RfcGetCurrentRow(fieldsTable, nullptr);

        // +1 for null terminator
        SAP_UC fieldname[fieldname_length + 1] = {0};
        SAP_UC fieldtext[fieldtext_length + 1] = {0};
        SAP_UC datatype[datatype_length + 1] = {0};
        SAP_UC leng[leng_length + 1] = {0};

        RfcGetChars(row, (SAP_UC *)sap_metadata_name_field, fieldname,
                    fieldname_length, nullptr);
        RfcGetChars(row, (SAP_UC *)sap_metadata_text_field, fieldtext,
                    fieldtext_length, nullptr);
        RfcGetChars(row, (SAP_UC *)sap_metadata_datatype_field, datatype,
                    datatype_length, nullptr);
        RfcGetChars(row, (SAP_UC *)sap_metadata_leng_field, leng, leng_length,
                    nullptr);

        std::unordered_map<std::string, std::string> field;
        field[sap_metadata_name_field] =
            utils::rtrim(utils::fromSAPUC(fieldname));
        field[sap_metadata_text_field] =
            utils::rtrim(utils::fromSAPUC(fieldtext));
        field[sap_metadata_datatype_field] =
            utils::rtrim(utils::fromSAPUC(datatype));
        field[sap_metadata_leng_field] = utils::rtrim(utils::fromSAPUC(leng));

        metadata.push_back(std::move(field));
    }

    RfcDestroyFunction(func, nullptr);

    return metadata;
}

// ### POC SMART CALLER ###
tables_type SapFunctionCaller::smart_call(const std::string &func,
                                          const params_type &params,
                                          const tables_type &tables) {
    // Only apply smart chunking for RFC_READ_TABLE
    if (func != "BBP_RFC_READ_TABLE") {
        return call(func, params, tables);
    }

    // Extract table name from params
    auto table_it = params.find("QUERY_TABLE");
    if (table_it == params.end()) {
        return call(func, params, tables);
    }
    std::string table_name = table_it->second;
    std::cout << "Querying Table: " << table_name << ", using smart call"
              << std::endl;

    // Extract columns from FIELDS table
    auto fields_it = tables.find("FIELDS");
    if (fields_it == tables.end()) {
        return call(func, params, tables);
    }
    std::vector<std::string> columns;
    for (const auto &field : fields_it->second) {
        auto fieldname_it = field.find("FIELDNAME");
        if (fieldname_it != field.end()) {
            columns.push_back(fieldname_it->second);
        }
    }

    // Extract where clause from OPTIONS table
    std::string where_clause;
    auto options_it = tables.find("OPTIONS");
    if (options_it != tables.end() && !options_it->second.empty()) {
        auto text_it = options_it->second[0].find("TEXT");
        if (text_it != options_it->second[0].end()) {
            where_clause = text_it->second;
        }
    }

    // Extract ROWCOUNT, ROWSKIPS, and DELIMITER from params
    int total_rowcount = 1000000;  // Default safe limit
    int rowskips = 0;
    std::string delimiter = "|";  // Default delimiter

    auto rowcount_it = params.find("ROWCOUNT");
    if (rowcount_it != params.end()) {
        total_rowcount = std::stoi(rowcount_it->second);
    }

    auto rowskips_it = params.find("ROWSKIPS");
    if (rowskips_it != params.end()) {
        rowskips = std::stoi(rowskips_it->second);
    }

    auto delimiter_it = params.find("DELIMITER");
    if (delimiter_it != params.end()) {
        delimiter = delimiter_it->second;
    }

    // Define row chunking parameters
    const int MAX_ROWS_PER_CHUNK = 100000;  // 100k rows per call
    int rows_per_chunk = std::min(total_rowcount, MAX_ROWS_PER_CHUNK);

    std::cout << "Total ROWCOUNT: " << total_rowcount
              << ", ROWSKIPS: " << rowskips
              << ", Rows per chunk: " << rows_per_chunk << std::endl;

    // 1. Get column metadata (lengths)
    metadata_type metadata = get_table_metadata(table_name);
    std::unordered_map<std::string, int> col_lengths;
    for (const auto &field : metadata) {
        auto it = field.find("FIELDNAME");
        auto it_len = field.find("LENG");
        if (it != field.end() && it_len != field.end()) {
            col_lengths[it->second] = std::stoi(it_len->second);
        }
    }

    // 2. Chunk columns so that sum of lengths <= 512
    const int MAX_CHUNK_LEN = 512;
    std::vector<std::vector<std::string>> column_chunks;
    std::vector<std::string> current_chunk;
    int current_len = 0;
    for (const auto &col : columns) {
        int len = col_lengths.count(col) ? col_lengths[col]
                                         : 10;  // fallback if not found
        if (current_len + len > MAX_CHUNK_LEN && !current_chunk.empty()) {
            column_chunks.push_back(current_chunk);
            current_chunk.clear();
            current_len = 0;
        }
        current_chunk.push_back(col);
        current_len += len;
    }
    if (!current_chunk.empty()) {
        column_chunks.push_back(current_chunk);
    }

    std::cout << "column_chunks: " << column_chunks.size() << std::endl;

    // 3. Process row chunks - for each row chunk, process all column chunks
    std::vector<std::unordered_map<std::string, std::string>> all_merged_rows;

    for (int row_offset = 0; row_offset < total_rowcount;
         row_offset += rows_per_chunk) {
        int current_rowcount =
            std::min(rows_per_chunk, total_rowcount - row_offset);
        int current_rowskips = rowskips + row_offset;

        std::cout << "Processing row chunk with offset: " << current_rowskips
                  << ", rowcount: " << current_rowcount << std::endl;

        // For this row chunk, process all column chunks
        std::vector<std::unordered_map<std::string, std::string>>
            row_chunk_results;
        bool first_chunk = true;
        size_t expected_rows = 0;

        for (const auto &col_chunk : column_chunks) {
            // Prepare FIELDS table for RFC_READ_TABLE
            std::vector<std::unordered_map<std::string, std::string>>
                fields_table;
            for (const auto &col : col_chunk) {
                fields_table.push_back({{"FIELDNAME", col}});
            }

            std::unordered_map<
                std::string,
                std::vector<std::unordered_map<std::string, std::string>>>
                chunk_tables = {{"FIELDS", fields_table}};
            if (!where_clause.empty()) {
                chunk_tables["OPTIONS"] = {{{"TEXT", where_clause}}};
            }

            // Prepare parameters for this row chunk
            std::unordered_map<std::string, std::string> chunk_params = params;
            chunk_params["ROWCOUNT"] = std::to_string(current_rowcount);
            chunk_params["ROWSKIPS"] = std::to_string(current_rowskips);
            chunk_params["DELIMITER"] = delimiter;

            // Call RFC_READ_TABLE for this column chunk
            auto result = call(func, chunk_params, chunk_tables);

            // Add raw DATA table result directly without parsing
            if (result.count("DATA")) {
                if (row_chunk_results.empty()) {
                    // First column chunk - just copy the raw data
                    row_chunk_results = result["DATA"];
                    expected_rows = result["DATA"].size();
                    first_chunk = false;

                    if (expected_rows == 0) {
                        std::cout
                            << "Warning: First column chunk returned no data"
                            << std::endl;
                        break;  // No point in processing other chunks if first
                                // chunk is empty
                    }
                } else {
                    // Subsequent column chunks - append raw data
                    row_chunk_results.insert(row_chunk_results.end(),
                                             result["DATA"].begin(),
                                             result["DATA"].end());
                }
            }
        }

        // Add the complete row chunk results to the overall results
        all_merged_rows.insert(all_merged_rows.end(), row_chunk_results.begin(),
                               row_chunk_results.end());
    }

    // 4. Convert back to the format expected by RFC_READ_TABLE (DATA table with
    // WA field)
    tables_type merged_result;

    // Just return the raw concatenated data without any processing
    merged_result["DATA"] = all_merged_rows;

    std::cout << "merged_result: " << merged_result.size() << std::endl;
    std::cout << "Total rows in merged result: " << all_merged_rows.size()
              << std::endl;

    // Final validation
    if (all_merged_rows.empty()) {
        std::cout << "Warning: No data was merged, returning empty result"
                  << std::endl;
    } else {
        std::cout << "Successfully merged " << all_merged_rows.size() << " rows"
                  << std::endl;
    }

    return merged_result;
}
