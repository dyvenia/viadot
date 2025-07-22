#include "SapErrorHandler.h"

#include <iostream>

#include "utils.h"

void SapErrorHandler::print_error(const RFC_ERROR_INFO &errorInfo) const {
    if (errorInfo.code != RFC_OK) {
        std::cerr << "RfcOpenConnection failed: "
                  << utils::fromSAPUC(errorInfo.message) << std::endl;
        std::cerr << "Error code: " << errorInfo.code << std::endl;
        std::cerr << "SAP error group: " << errorInfo.group << std::endl;
        std::cerr << "SAP error key: " << utils::fromSAPUC(errorInfo.key)
                  << std::endl;
        std::cerr << "SAP error message: "
                  << utils::fromSAPUC(errorInfo.message) << std::endl;
    }
}