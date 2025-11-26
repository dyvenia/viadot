"""Source imports."""

from importlib.util import find_spec

from ._duckdb import DuckDB
from ._trino import Trino
from .azure_sql import AzureSQL
from .bigquery import BigQuery
from .business_core import BusinessCore
from .cloud_for_customers import CloudForCustomers
from .customer_gauge import CustomerGauge
from .epicor import Epicor
from .eurostat import Eurostat
from .exchange_rates import ExchangeRates
from .genesys import Genesys
from .hubspot import Hubspot
from .matomo import Matomo
from .mediatool import Mediatool
from .mindful import Mindful
from .outlook import Outlook
from .salesforce import Salesforce
from .sftp import Sftp
from .sharepoint import Sharepoint, SharepointList
from .smb import SMB
from .sql_server import SQLServer
from .sqlite import SQLite
from .supermetrics import Supermetrics
from .tm1 import TM1
from .uk_carbon_intensity import UKCarbonIntensity
from .vid_club import VidClub


__all__ = [
    "SMB",
    "TM1",
    "AzureSQL",
    "BigQuery",
    "BusinessCore",
    "CloudForCustomers",
    "CustomerGauge",
    "DuckDB",
    "Epicor",
    "Eurostat",
    "ExchangeRates",
    "Genesys",
    "Hubspot",
    "Matomo",
    "Mediatool",
    "Mindful",
    "Outlook",
    "SQLServer",
    "SQLite",
    "Salesforce",
    "Sftp",
    "Sharepoint",
    "SharepointList",
    "Supermetrics",
    "Trino",
    "UKCarbonIntensity",
    "VidClub",
]

if find_spec("adlfs"):
    from viadot.sources.azure_data_lake import AzureDataLake  # noqa: F401

    __all__.extend(["AzureDataLake"])

if find_spec("redshift_connector"):
    from viadot.sources.redshift_spectrum import RedshiftSpectrum  # noqa: F401

    __all__.extend(["RedshiftSpectrum"])

if find_spec("s3fs"):
    from viadot.sources._minio import MinIO  # noqa: F401
    from viadot.sources.s3 import S3  # noqa: F401

    __all__.extend(["S3", "MinIO"])

if find_spec("pyrfc"):
    from viadot.sources.sap_bw import SAPBW  # noqa: F401
    from viadot.sources.sap_rfc import SAPRFC  # noqa: F401

    __all__.extend(["SAPBW", "SAPRFC"])

if find_spec("pyspark"):
    from viadot.sources.databricks import Databricks  # noqa: F401

    __all__.append("Databricks")
