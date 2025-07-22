#pragma once

#include <map>
#include <string>
#include <vector>

#include "SapRfcConnector.h"
#include "SqlParser.h"

class SapFunctionCaller {
   public:
    SapFunctionCaller(SapRfcConnector *connector);

    std::vector<std::map<std::string, std::string>> get_function_description(
        const std::string &function_name);

    std::map<std::string, std::vector<std::map<std::string, std::string>>> call(
        const std::string &func,
        const std::map<std::string, std::string> &params,
        const std::map<std::string,
                       std::vector<std::map<std::string, std::string>>>
            &tables = {});

    std::vector<std::map<std::string, std::string>> get_table_metadata(
        const std::string &table_name);

   private:
    SapRfcConnector *connector_;
};