#include "SapErrorHandler.h"
#include "SapFunctionCaller.h"
#include "SapRfcConnector.h"
#include "SqlParser.h"
#include "utils.h"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>


namespace py = pybind11;
using namespace pybind11::literals;

PYBIND11_MODULE(sap_rfc_connector, m) {
  py::class_<SapRfcConnector>(m, "SapRfcConnector")
      .def(py::init<>())
      .def("connect", &SapRfcConnector::connect, py::arg("user"),
           py::arg("passwd"), py::arg("ashost"), py::arg("sysnr"),
           "Connect to SAP using SAP NetWeaver RFC SDK")
      .def("con", &SapRfcConnector::con,
           "Get the RFC connection handle (internal use)")
      .def("check_connection", &SapRfcConnector::check_connection,
           "Ping the SAP system to check connection")
      .def("close_connection", &SapRfcConnector::close_connection,
           "Close the SAP connection")
      .def("get_library_info", &SapRfcConnector::get_library_info,
           "Get library information from SAP");

  py::class_<SapFunctionCaller>(m, "SapFunctionCaller")
      .def(py::init<SapRfcConnector *>())
      .def("get_function_description",
           &SapFunctionCaller::get_function_description,
           py::arg("function_name"), "Get function metadata from SAP")
      .def("call", &SapFunctionCaller::call, py::arg("func"), py::arg("params"),
           py::arg("tables"),
           "Call a function with parameters and return results as a map")
      .def("get_table_metadata", &SapFunctionCaller::get_table_metadata,
           py::arg("table_name"), "Get table metadata from SAP")
      .def("rfc_read_table_query", &SapFunctionCaller::rfc_read_table_query,
           py::arg("sql"), py::arg("sep") = "", py::arg("rowskips") = 0,
           py::arg("rowcount") = 0, "RFC_READ_TABLE query");
}