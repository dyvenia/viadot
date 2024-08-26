"""Import flows."""

from .bigquery_to_adls import bigquery_to_adls
from .cloud_for_customers_to_adls import cloud_for_customers_to_adls
from .cloud_for_customers_to_databricks import cloud_for_customers_to_databricks
from .duckdb_to_parquet import duckdb_to_parquet
from .duckdb_to_sql_server import duckdb_to_sql_server
from .duckdb_transform import duckdb_transform
from .exchange_rates_to_adls import exchange_rates_to_adls
from .exchange_rates_to_databricks import exchange_rates_to_databricks
from .genesys_to_adls import genesys_to_adls
from .hubspot_to_adls import hubspot_to_adls
from .mindful_to_adls import mindful_to_adls
from .outlook_to_adls import outlook_to_adls
from .sap_to_parquet import sap_to_parquet
from .sap_to_redshift_spectrum import sap_to_redshift_spectrum
from .sharepoint_to_adls import sharepoint_to_adls
from .sharepoint_to_databricks import sharepoint_to_databricks
from .sharepoint_to_redshift_spectrum import sharepoint_to_redshift_spectrum
from .sharepoint_to_s3 import sharepoint_to_s3
from .sql_server_to_minio import sql_server_to_minio
from .transform import transform
from .transform_and_catalog import transform_and_catalog

__all__ = [
    "bigquery_to_adls",
    "cloud_for_customers_to_adls",
    "cloud_for_customers_to_databricks",
    "duckdb_to_parquet",
    "duckdb_to_sql_server",
    "duckdb_transform",
    "exchange_rates_to_adls",
    "exchange_rates_to_databricks",
    "genesys_to_adls",
    "hubspot_to_adls",
    "mindful_to_adls",
    "outlook_to_adls",
    "sap_to_parquet",
    "sap_to_redshift_spectrum",
    "sharepoint_to_adls",
    "sharepoint_to_databricks",
    "sharepoint_to_redshift_spectrum",
    "sharepoint_to_s3",
    "sql_server_to_minio",
    "transform",
    "transform_and_catalog",
]
