#include <algorithm>
#include <sstream>

#include "SqlParser.h"

SQLQueryParts SqlParser::parse_sql_select(const std::string &sql) {
  SQLQueryParts parts;
  std::string upper_sql = sql;
  std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(),
                 ::toupper);

  auto select_pos = upper_sql.find("SELECT ");
  auto from_pos = upper_sql.find(" FROM ");
  auto where_pos = upper_sql.find(" WHERE ");

  if (select_pos == std::string::npos || from_pos == std::string::npos)
    throw std::invalid_argument("Invalid SQL SELECT syntax");

  std::string cols_str =
      sql.substr(select_pos + 7, from_pos - (select_pos + 7));
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