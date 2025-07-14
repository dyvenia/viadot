cmake_minimum_required(VERSION 3.14)
project(sap_rfc_connector_test)

set(CMAKE_CXX_STANDARD 17)

# Fetch GoogleTest
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Add the test executable
add_executable(sap_rfc_connector_test
    sap_rfc_connector_test.cpp
    mock_sap_rfc_connector.h
)

target_include_directories(sap_rfc_connector_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(sap_rfc_connector_test
    gtest
    gtest_main
    pthread
)