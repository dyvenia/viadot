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
    install_requires=["sharepy>=2.0.0,<3.0.0", "pyodbc>=4.0.32,<5.0.0"],
    extras=extras,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
