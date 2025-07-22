#include <iostream>

#include "SapErrorHandler.h"
#include "utils.h"

void SapErrorHandler::print_error(const RFC_ERROR_INFO &errorInfo) const {
  if (errorInfo.code != RFC_OK) {
    std::cerr << "RfcOpenConnection failed: " << fromSAPUC(errorInfo.message)
              << std::endl;
    std::cerr << "Error code: " << errorInfo.code << std::endl;
    std::cerr << "SAP error group: " << errorInfo.group << std::endl;
    std::cerr << "SAP error key: " << fromSAPUC(errorInfo.key) << std::endl;
    std::cerr << "SAP error message: " << fromSAPUC(errorInfo.message)
              << std::endl;
  }
}