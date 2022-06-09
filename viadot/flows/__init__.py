from .adls_gen1_to_azure_sql import ADLSGen1ToAzureSQL
from .adls_gen1_to_azure_sql_new import ADLSGen1ToAzureSQLNew
from .adls_gen1_to_gen2 import ADLSGen1ToGen2
from .adls_to_azure_sql import ADLSToAzureSQL
from .azure_sql_transform import AzureSQLTransform
from .flow_of_flows import Pipeline
from .supermetrics_to_adls import SupermetricsToADLS
from .supermetrics_to_azure_sql import SupermetricsToAzureSQL
from .adls_container_to_container import ADLSContainerToContainer
from .sharepoint_to_adls import SharepointToADLS
from .cloud_for_customers_report_to_adls import CloudForCustomersReportToADLS
from .aselite_to_adls import ASELiteToADLS
from .bigquery_to_adls import BigQueryToADLS
from .outlook_to_adls import OutlookToADLS

try:
    from .sap_to_duckdb import SAPToDuckDB
except ImportError:
    pass

from .duckdb_transform import DuckDBTransform
from .duckdb_to_sql_server import DuckDBToSQLServer
from .multiple_flows import MultipleFlows

try:
    from .sap_rfc_to_adls import SAPRFCToADLS
except ImportError:
    pass

from .sql_server_to_duckdb import SQLServerToDuckDB
from .epicor_to_duckdb import EpicorOrdersToDuckDB
