"""'test_bigquery.py'."""

from viadot.orchestration.prefect.flows import bigquery_to_adls


def test_salesforce_to_adls():
    """Test SalesForce prefect flow."""
    state = bigquery_to_adls(
        azure_key_vault_secret="bigquery",
        dataset_name="VX_CXU_Data_Feeds",
        table_name="VX_CXU_Operational_CRM_daily_EXPORT",
        adls_path="raw/dyvenia_sandbox/bigquery/bigquery.csv",
        adls_azure_key_vault_secret="app-azure-cr-datalakegen2-dev",
        adls_path_overwrite=True,
    )
    all_successful = all(s.type == "COMPLETED" for s in state)
    assert all_successful
