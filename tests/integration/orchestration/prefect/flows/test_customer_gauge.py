"""'test_customer_gauge.py'."""

from viadot.orchestration.prefect.flows import customer_gauge_to_adls


def test_sftp_to_adls(azure_key_vault_secret, adls_azure_key_vault_secret):
    """Test Customer Gauge API prefect flow."""
    state = customer_gauge_to_adls(
        azure_key_vault_secret=azure_key_vault_secret,
        unpack_by_field_reference_cols=["properties"],
        unpack_by_nested_dict_transformer=[],
        adls_path="raw/dyvenia_sandbox/customer_gauge/customer_gauge.parquet",
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_path_overwrite=True,
    )
    all_successful = all(s.type == "COMPLETED" for s in state)
    assert all_successful, "Not all tasks in the flow completed successfully."
