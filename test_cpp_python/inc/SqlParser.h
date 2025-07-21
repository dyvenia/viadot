#pragma once

#include <string>
#include <vector>

struct SQLQueryParts {
    std::string tableName;
    std::vector<std::string> columns;
    std::string whereClause;
};

class SqlParser {
public:
    static SQLQueryParts parse_sql_select(const std::string& sql);
}; 
