#include "utils.h"

#include <codecvt>
#include <locale>
#include <string>

namespace utils {
std::u16string to_u16string(const std::string &utf8_str) {
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;

    return converter.from_bytes(utf8_str);
}

std::string fromSAPUC(const SAP_UC *uc_str) {
    if (!uc_str) return "";

    try {
        std::u16string u16str(uc_str);  // we assume it is null-terminated

        // Use a more robust conversion with error handling
        std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t>
            convert;
        convert.from_bytes(
            convert.to_bytes(u16str));  // This will throw if invalid UTF-16

        return convert.to_bytes(u16str);
    } catch (const std::exception &e) {
        // If UTF-16 conversion fails, try a more lenient approach
        // Convert each SAP_UC to a byte and handle as raw bytes
        std::string result;
        const SAP_UC *ptr = uc_str;
        while (*ptr != 0) {
            // Convert SAP_UC to byte, handling potential encoding issues
            unsigned char byte = static_cast<unsigned char>(*ptr & 0xFF);
            if (byte >= 0x20 && byte <= 0x7E) {
                // Printable ASCII character
                result += static_cast<char>(byte);
            } else if (byte == 0x00) {
                // Null terminator
                break;
            } else {
                // Non-printable or special character - replace with space or
                // skip
                result += ' ';
            }
            ++ptr;
        }
        return result;
    }
}

std::string rfcDirectionToString(RFC_DIRECTION direction) {
    switch (direction) {
        case RFC_IMPORT:
            return "IMPORT";
        case RFC_EXPORT:
            return "EXPORT";
        case RFC_CHANGING:
            return "CHANGING";
        case RFC_TABLES:
            return "TABLES";
        default:
            return "UNKNOWN";
    }
}

std::string rfcTypeToString(RFCTYPE type) {
    switch (type) {
        case RFCTYPE_CHAR:
            return "RFCTYPE_CHAR";
        case RFCTYPE_DATE:
            return "RFCTYPE_DATE";
        case RFCTYPE_BCD:
            return "RFCTYPE_BCD";
        case RFCTYPE_TIME:
            return "RFCTYPE_TIME";
        case RFCTYPE_BYTE:
            return "RFCTYPE_BYTE";
        case RFCTYPE_TABLE:
            return "RFCTYPE_TABLE";
        case RFCTYPE_NUM:
            return "RFCTYPE_NUM";
        case RFCTYPE_FLOAT:
            return "RFCTYPE_FLOAT";
        case RFCTYPE_INT:
            return "RFCTYPE_INT";
        case RFCTYPE_INT2:
            return "RFCTYPE_INT2";
        case RFCTYPE_INT1:
            return "RFCTYPE_INT1";
        case RFCTYPE_STRING:
            return "RFCTYPE_STRING";
        case RFCTYPE_STRUCTURE:
            return "RFCTYPE_STRUCTURE";
        case RFCTYPE_XSTRING:
            return "RFCTYPE_XSTRING";
        default:
            return "UNKNOWN";
    }
}

std::string rtrim(const std::string &s) {
    auto end = s.find_last_not_of(" \t\r\n");

    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

std::string rtrim_sapstring(const SAP_UC *uc) {
    std::string s = fromSAPUC(uc);
    auto end = s.find_last_not_of(" \t\r\n");

    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

}  // namespace utils
