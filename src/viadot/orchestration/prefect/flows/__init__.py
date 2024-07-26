"""Import flows."""

from .cloud_for_customers_to_adls import cloud_for_customers_to_adls  # noqa: F401
from .cloud_for_customers_to_databricks import (
    cloud_for_customers_to_databricks,  # noqa: F401;
)
from .duckdb_to_parquet import duckdb_to_parquet  # noqa: F401
from .duckdb_to_sql_server import duckdb_to_sql_server  # noqa: F401
from .duckdb_transform import duckdb_transform  # noqa: F401
from .exchange_rates_to_adls import exchange_rates_to_adls  # noqa: F401
from .exchange_rates_to_databricks import exchange_rates_to_databricks  # noqa: F401
from .genesys_to_adls import genesys_to_adls  # noqa: F401
from .hubspot_to_adls import hubspot_to_adls  # noqa: F401
from .mediatool_to_adls import mediatool_to_adls  # noqa: F401
from .mindful_to_adls import mindful_to_adls  # noqa: F401
from .outlook_to_adls import outlook_to_adls  # noqa: F401
from .sap_to_parquet import sap_to_parquet  # noqa: F401
from .sap_to_redshift_spectrum import sap_to_redshift_spectrum  # noqa: F401
from .sharepoint_to_adls import sharepoint_to_adls  # noqa: F401
from .sharepoint_to_databricks import sharepoint_to_databricks  # noqa: F401
from .sharepoint_to_redshift_spectrum import (
    sharepoint_to_redshift_spectrum,  # noqa: F401
)
from .sharepoint_to_s3 import sharepoint_to_s3  # noqa: F401
from .sql_server_to_minio import sql_server_to_minio  # noqa: F401
from .transform import transform  # noqa: F401
from .transform_and_catalog import transform_and_catalog  # noqa: F401
