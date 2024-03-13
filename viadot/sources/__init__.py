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

try:
    from .sap_rfc import SAPRFC, SAPRFCV2
except ImportError:
    pass

from .eurostat import Eurostat
