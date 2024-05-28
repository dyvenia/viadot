from importlib.util import find_spec

from viadot.sources.cloud_for_customers import CloudForCustomers
from viadot.sources.exchange_rates import ExchangeRates
from viadot.sources.genesys import Genesys
from viadot.sources.sharepoint import Sharepoint
from viadot.sources.trino_source import Trino

__all__ = [
    "CloudForCustomers",
    "ExchangeRates",
    "Genesys",
    "Sharepoint",
    "Trino",
]

if find_spec("adlfs"):
    from viadot.sources.azure_data_lake import AzureDataLake  # noqa

    __all__.extend(["AzureDataLake"])

if find_spec("redshift_connector"):
    from viadot.sources.redshift_spectrum import RedshiftSpectrum  # noqa

    __all__.extend(["RedshiftSpectrum"])

if find_spec("s3fs"):
    from viadot.sources.s3 import S3  # noqa

    __all__.extend(["S3"])

if find_spec("s3fs"):
    from viadot.sources.minio_source import MinIO  # noqa

    __all__.extend(["MinIO"])


if find_spec("pyrfc"):
    from viadot.sources.sap_rfc import SAPRFC, SAPRFCV2  # noqa

    __all__.extend(["SAPRFC", "SAPRFCV2"])

if find_spec("pyspark"):
    from viadot.sources.databricks import Databricks  # noqa

    __all__.append("Databricks")
