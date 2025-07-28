#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "SapRfcConnector.h"

using tables_type = std::unordered_map<
    std::string, std::vector<std::unordered_map<std::string, std::string>>>;
using params_type = std::unordered_map<std::string, std::string>;
using metadata_type = std::vector<std::unordered_map<std::string, std::string>>;

class SapFunctionCaller {
   public:
    SapFunctionCaller(SapRfcConnector *connector);

    metadata_type get_function_description(const std::string &function_name);

    tables_type call(const std::string &func, const params_type &params,
                     const tables_type &tables = {});

    metadata_type get_table_metadata(const std::string &table_name);

    // POC: Smart SAP RFC chunked call
    tables_type smart_call(const std::string &func, const params_type &params,
                           const tables_type &tables = {});

   private:
    SapRfcConnector *connector_;
};