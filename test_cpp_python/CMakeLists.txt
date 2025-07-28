cmake_minimum_required(VERSION 3.10)
project(sap_rfc_connector)

# Allow user to specify SAP NWRFC SDK root, or use default
set(SAP_NWRFCSKD_ROOT "/usr/local/sap/nwrfcsdk" CACHE PATH "Path to SAP NetWeaver RFC SDK root")
set(SAP_NWRFCSKD_INCLUDE "${SAP_NWRFCSKD_ROOT}/include")
set(SAP_NWRFCSKD_LIB "${SAP_NWRFCSKD_ROOT}/lib")

# Check if the include directory exists
if(NOT EXISTS "${SAP_NWRFCSKD_INCLUDE}/sapnwrfc.h")
    message(FATAL_ERROR "Could not find sapnwrfc.h in ${SAP_NWRFCSKD_INCLUDE}. Please set SAP_NWRFCSKD_ROOT correctly.")
endif()

include_directories(${SAP_NWRFCSKD_INCLUDE})
include_directories(inc)
link_directories(${SAP_NWRFCSKD_LIB})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_CXX_EXTENSIONS OFF)

find_package(pybind11 REQUIRED)

file(GLOB SAP_SRC_FILES src/*.cpp)
pybind11_add_module(sap_rfc_connector ${SAP_SRC_FILES})

# Link SAP NWRFC and ICU libraries
# Set rpath so the shared library can find SAP libs at runtime
set_target_properties(sap_rfc_connector PROPERTIES
    INSTALL_RPATH "${SAP_NWRFCSKD_LIB}"
    BUILD_WITH_INSTALL_RPATH TRUE
)

target_link_libraries(sap_rfc_connector
    PRIVATE
    sapnwrfc
)

# Define SAP Unicode support
target_compile_definitions(sap_rfc_connector PRIVATE SAPwithUNICODE)