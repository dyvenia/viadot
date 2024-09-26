"""Source imports."""

from importlib.util import find_spec

from ._duckdb import DuckDB
from ._trino import Trino
from .cloud_for_customers import CloudForCustomers
from .epicor import Epicor
from .exchange_rates import ExchangeRates
from .genesys import Genesys
from .hubspot import Hubspot
from .mindful import Mindful
from .outlook import Outlook
from .sharepoint import Sharepoint
from .sql_server import SQLServer
from .uk_carbon_intensity import UKCarbonIntensity


__all__ = [
    "CloudForCustomers",
    "Epicor",
    "ExchangeRates",
    "Genesys",
    "Hubspot",
    "Mindful",
    "Outlook",
    "Sharepoint",
    "Trino",
    "SQLServer",
    "UKCarbonIntensity",
]

if find_spec("adlfs"):
    from viadot.sources.azure_data_lake import AzureDataLake  # noqa: F401

    __all__.extend(["AzureDataLake"])

if find_spec("duckdb"):
    from viadot.sources._duckdb import DuckDB  # noqa: F401

    __all__.extend(["DuckDB"])

if find_spec("redshift_connector"):
    from viadot.sources.redshift_spectrum import RedshiftSpectrum  # noqa: F401

    __all__.extend(["RedshiftSpectrum"])

if find_spec("s3fs"):
    from viadot.sources.s3 import S3  # noqa: F401

    __all__.extend(["S3"])

if find_spec("s3fs"):
    from viadot.sources.minio import MinIO  # noqa: F401

    __all__.extend(["MinIO"])


if find_spec("pyrfc"):
    from viadot.sources.sap_bw import SAPBW  # noqa: F401
    from viadot.sources.sap_rfc import SAPRFC, SAPRFCV2  # noqa: F401

    __all__.extend(["SAPRFC", "SAPRFCV2", "SAPBW"])

if find_spec("pyspark"):
    from viadot.sources.databricks import Databricks  # noqa: F401

    __all__.append("Databricks")
