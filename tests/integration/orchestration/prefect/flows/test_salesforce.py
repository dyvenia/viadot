"""'test_salesforce.py'."""

from viadot.orchestration.prefect.flows import salesforce_to_adls


def test_salesforce_to_adls(
    azure_key_vault_secret, adls_path, adls_azure_key_vault_secret
):
    """Test Salesforce prefect flow."""
    state = salesforce_to_adls(
        azure_key_vault_secret=azure_key_vault_secret,
        env="dev",
        table="Contact",
        adls_path=adls_path,
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_path_overwrite=True,
    )
    all_successful = all(s.type == "COMPLETED" for s in state)
    assert all_successful, "Not all tasks in the flow completed successfully."
