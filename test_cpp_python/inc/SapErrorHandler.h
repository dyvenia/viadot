#pragma once

#include "sapnwrfc.h"

class SapErrorHandler {
public:
  void print_error(const RFC_ERROR_INFO &errorInfo) const;
};
