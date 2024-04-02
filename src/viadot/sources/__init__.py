from importlib.util import find_spec

from .azure_data_lake import AzureDataLake
from .cloud_for_customers import CloudForCustomers
from .databricks import Databricks
from .exchange_rates import ExchangeRates
from .genesys import Genesys
from .minio import MinIO
from .redshift_spectrum import RedshiftSpectrum
from .s3 import S3
from .sharepoint import Sharepoint
from .trino import Trino

__all__ = [
    "AzureDataLake",
    "CloudForCustomers",
    "Databricks",
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
