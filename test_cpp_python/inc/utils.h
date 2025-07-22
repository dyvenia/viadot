#pragma once

// Utility function declarations
std::u16string to_u16string(const std::string &utf8_str);
std::string fromSAPUC(const SAP_UC *uc_str);
std::string rfcDirectionToString(RFC_DIRECTION direction);
std::string rfcTypeToString(RFCTYPE type);
