from importlib.util import find_spec

from viadot.sources.azure_data_lake import AzureDataLake
from viadot.sources.cloud_for_customers import CloudForCustomers
from viadot.sources.exchange_rates import ExchangeRates
from viadot.sources.genesys import Genesys
from viadot.sources.minio import MinIO
from viadot.sources.redshift_spectrum import RedshiftSpectrum
from viadot.sources.s3 import S3
from viadot.sources.sharepoint import Sharepoint
from viadot.sources.trino import Trino

__all__ = [
    "AzureDataLake",
    "CloudForCustomers",
    "ExchangeRates",
    "Genesys",
    "MinIO",
    "RedshiftSpectrum",
    "S3",
    "Sharepoint",
    "Trino",
]

if find_spec("pyrfc"):
    __all__.extend(["SAPRFC", "SAPRFCV2"])

if find_spec("databricks-connect"):
    __all__.extend(["Databricks"])