import os
import re

import setuptools


def get_version(package: str):
    """Return package version as listed in `__version__` in `init.py`."""
    init_py = open(os.path.join(package, "__init__.py")).read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


with open("README.md", "r") as fh:
    long_description = fh.read()

extras = {
    "sap": ["pyrfc==2.5.0", "sql-metadata==2.3.0"],
}

setuptools.setup(
    name="viadot",
    version=get_version("viadot"),
    author="Alessio Civitillo",
    maintainer="Michal Zawadzki",
    maintainer_email="mzawadzki@dyvenia.com",
    description="A simple data ingestion library to guide data flows from some places to other places",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dyvenia/viadot",
    packages=setuptools.find_packages(),
    install_requires=[
        "azure-core==1.25.0",
        "azure-storage-blob==12.13.1",
        "awswrangler==2.19.0",
        "s3fs==2022.11.0",
        "boto3==1.24.59",
        "pandas==1.4.4",
        "pyarrow==10.0.1",
        "pyodbc>=4.0.34, <4.1.0",
        "openpyxl==3.0.10",
        "jupyterlab==3.2.4",
        "azure-identity==1.11.*",
        "matplotlib",
        "adlfs==2022.9.1",
        "Shapely==1.8.0",
        "imagehash==4.2.1",
        "visions==0.7.5",
        "sharepy>=2.0.0, <2.1.0",
        "simple_salesforce==1.11.5",
        "sql-metadata==2.3.0",
        "duckdb==0.5.1",
        "sendgrid==6.9.7",
        "pandas-gbq==0.19.1",
        "databricks-connect==11.3.*",
        "pyyaml>=6.0.1",
        "pydantic==1.10.11",
        "aiolimiter==1.0.0",
        "trino==0.326.*",
        "sqlalchemy==2.0.*",
        "minio>=7.0, <8.0",
    ],
    extras=extras,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
