from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext
import os
import shutil
import sys

def clean_build():
    build_dirs = ['build', 'dist']
    for build_dir in build_dirs:
        if os.path.exists(build_dir):
            shutil.rmtree(build_dir)
            print(f"Removed {build_dir} directory")

clean_build()

SAPNWRFC_HOME = os.environ.get("SAPNWRFC_HOME")
if not SAPNWRFC_HOME:
    print("Warning: Environment variable SAPNWRFC_HOME not set.")
    print("This variable should point to the root directory of the SAP NWRFC Library.")
    print("Building without SAP NW RFC SDK linking...")
    SAPNWRFC_HOME = None

if sys.platform.startswith("win"):
    LIBS = ["sapnwrfc", "libsapucum"] if SAPNWRFC_HOME else []
    MACROS = [
        ("SAPonNT", None),
        ("_CRT_NON_CONFORMING_SWPRINTFS", None),
        ("_CRT_SECURE_NO_DEPRECATES", None),
        ("_CRT_NONSTDC_NO_DEPRECATE", None),
        ("_AFXDLL", None),
        ("WIN32", None),
        ("_WIN32_WINNT", "0x0502"),
        ("WIN64", None),
        ("_AMD64_", None),
        ("NDEBUG", None),
        ("SAPwithUNICODE", None),
        ("UNICODE", None),
        ("_UNICODE", None),
        ("SAPwithTHREADS", None),
        ("_ATL_ALLOW_CHAR_UNSIGNED", None),
        ("_LARGEFILE_SOURCE", None),
        ("_CONSOLE", None),
        ("SAP_PLATFORM_MAKENAME", "ntintel"),
    ]
    COMPILE_ARGS = [
        "/EHsc",
        "/MD",
        "/O2",
        "/Ob2",
        "/DNDEBUG",
        "/DWIN32",
        "/D_WINDOWS",
        "/D_AMD64_",
        "/DWIN64",
        "/D_CRT_SECURE_NO_WARNINGS",
        "/D_CRT_NONSTDC_NO_DEPRECATE",
        "/D_CRT_NON_CONFORMING_SWPRINTFS",
        "/D_AFXDLL",
        "/DSAPonNT",
        "/DSAPwithUNICODE",
        "/DSAPwithTHREADS",
        "/DUNICODE",
        "/D_UNICODE",
        "/D_ATL_ALLOW_CHAR_UNSIGNED",
        "/D_LARGEFILE_SOURCE",
        "/D_CONSOLE",
        "/DSAP_PLATFORM_MAKENAME=ntintel",
    ]
    LINK_ARGS = []
    INCLUDE_DIRS = ["inc"]
    
    if SAPNWRFC_HOME:
        COMPILE_ARGS.append(f"/I{SAPNWRFC_HOME}/include")
        LINK_ARGS.append(f"/LIBPATH:{SAPNWRFC_HOME}/lib")
        INCLUDE_DIRS.append(f"{SAPNWRFC_HOME}/include")
else:
    LIBS = ["sapnwrfc", "sapucum"] if SAPNWRFC_HOME else []
    MACROS = [
        ("NDEBUG", None),
        ("_LARGEFILE_SOURCE", None),
        ("_CONSOLE", None),
        ("_FILE_OFFSET_BITS", 64),
        ("SAPonUNIX", None),
        ("SAPwithUNICODE", None),
        ("SAPwithTHREADS", None),
        ("SAPonLIN", None),
    ]
    COMPILE_ARGS = [
        "-Wall",
        "-O2",
        "-fexceptions",
        "-funsigned-char",
        "-fno-strict-aliasing",
        "-Wall",
        "-Wno-uninitialized",
        "-Wno-deprecated-declarations",
        "-Wno-unused-function",
        "-Wcast-align",
        "-fPIC",
        "-pthread",
        "-minline-all-stringops",
    ]
    LINK_ARGS = []
    INCLUDE_DIRS = ["inc"]
    
    if SAPNWRFC_HOME:
        COMPILE_ARGS.append(f"-I{SAPNWRFC_HOME}/include")
        LINK_ARGS.append(f"-L{SAPNWRFC_HOME}/lib")
        INCLUDE_DIRS.append(f"{SAPNWRFC_HOME}/include")

ext_modules = [
    Pybind11Extension(
        "sap_rfc_connector",
        [
            "src/SapErrorHandler.cpp",
            "src/SapFunctionCaller.cpp",
            "src/SapRfcConnector.cpp",
            "src/SqlParser.cpp",
            "src/utils.cpp",
            "src/sap_rfc_connector_pybind.cpp",
        ],
        include_dirs=INCLUDE_DIRS,
        libraries=LIBS,
        cxx_std=17,
        extra_compile_args=COMPILE_ARGS,
        extra_link_args=LINK_ARGS,
        define_macros=MACROS,
    ),
]

setup(
    name="sap_rfc_connector",
    version="0.1.0",
    author="Dominik Tyrala",
    author_email="dominik.tyrala@dyvenia.com",
    description="SAP RFC wrapper written in C++ with Python bindings",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: C++",
        "Operating System :: OS Independent",
    ],
    packages=[],
)