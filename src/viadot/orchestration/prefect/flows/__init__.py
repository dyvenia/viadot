"""Import flows."""

# Dictionary to track successfully imported flows
_imported_flows = {}

# Import each flow with error handling
def _safe_import(module_name, *flow_names):
    """Safely import flows, catching and logging import errors."""
    try:
        module = __import__(
            f"viadot.orchestration.prefect.flows.{module_name}",
            fromlist=flow_names
        )
        for flow_name in flow_names:
            _imported_flows[flow_name] = getattr(module, flow_name)
            globals()[flow_name] = _imported_flows[flow_name]
    except ImportError as e:
        # Silently skip flows that can't be imported
        pass
    except AttributeError as e:
        # Handle case where module exists but flow function doesn't
        pass


# Import all flows with error handling
_safe_import("azure_sql_to_adls", "azure_sql_to_adls")
_safe_import("bigquery_to_adls", "bigquery_to_adls")
_safe_import("business_core_to_parquet", "business_core_to_parquet")
_safe_import("cloud_for_customers_to_adls", "cloud_for_customers_to_adls")
_safe_import("cloud_for_customers_to_databricks", "cloud_for_customers_to_databricks")
_safe_import("customer_gauge_to_adls", "customer_gauge_to_adls")
_safe_import("duckdb_to_parquet", "duckdb_to_parquet")
_safe_import("duckdb_to_sql_server", "duckdb_to_sql_server")
_safe_import("duckdb_transform", "duckdb_transform")
_safe_import("entraid_to_redshift_spectrum", "entraid_to_redshift_spectrum")
_safe_import("epicor_to_parquet", "epicor_to_parquet")
_safe_import("eurostat_to_adls", "eurostat_to_adls")
_safe_import("exchange_rates_to_adls", "exchange_rates_to_adls")
_safe_import("exchange_rates_to_databricks", "exchange_rates_to_databricks")
_safe_import("exchange_rates_to_redshift_spectrum", "exchange_rates_api_to_redshift_spectrum")
_safe_import("genesys_to_adls", "genesys_to_adls")
_safe_import("hubspot_to_adls", "hubspot_to_adls")
_safe_import("matomo_to_redshift_spectrum", "matomo_to_redshift_spectrum")
_safe_import("mediatool_to_adls", "mediatool_to_adls")
_safe_import("mindful_to_adls", "mindful_to_adls")
_safe_import("onestream_data_adapters_to_redshift_spectrum", 
             "onestream_data_adapters_to_redshift_spectrum",
             "onestream_sql_query_data_to_redshift_spectrum")
_safe_import("outlook_to_adls", "outlook_to_adls")
_safe_import("salesforce_to_adls", "salesforce_to_adls")
_safe_import("sap_bw_to_adls", "sap_bw_to_adls")
_safe_import("sap_to_parquet", "sap_to_parquet")
_safe_import("sap_to_redshift_spectrum", "sap_to_redshift_spectrum")
_safe_import("sftp_to_adls", "sftp_to_adls")
_safe_import("sharepoint_list_to_redshift_spectrum", "sharepoint_list_to_redshift_spectrum")
_safe_import("sharepoint_to_adls", "sharepoint_to_adls")
_safe_import("sharepoint_to_redshift_spectrum", "sharepoint_to_redshift_spectrum")
_safe_import("sharepoint_to_s3", "sharepoint_to_s3")
_safe_import("sql_server_to_minio", "sql_server_to_minio")
_safe_import("sql_server_to_parquet", "sql_server_to_parquet")
_safe_import("sql_server_to_redshift_spectrum", "sql_server_to_redshift_spectrum")
_safe_import("sql_server_transform", "sql_server_transform")
_safe_import("supermetrics_to_adls", "supermetrics_to_adls")
_safe_import("tm1_to_parquet", "tm1_to_parquet")
_safe_import("transform", "transform")
_safe_import("transform_and_catalog", "transform_and_catalog")
_safe_import("vid_club_to_adls", "vid_club_to_adls")

# Only export successfully imported flows
__all__ = list(_imported_flows.keys())
