"""'test_sftp.py'."""

from viadot.orchestration.prefect.flows import sftp_to_adls


def test_sftp_to_adls(azure_key_vault_secret, adls_azure_key_vault_secret):
    """Test SFTP server prefect flow."""
    state = sftp_to_adls(
        azure_key_vault_secret=azure_key_vault_secret,
        file_name="Data Lake- V-F Products with Prices.tsv",
        adls_path="raw/dyvenia_sandbox/sftp/sftp.csv",
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_path_overwrite=True,
    )
    all_successful = all(s.type == "COMPLETED" for s in state)
    assert all_successful, "Not all tasks in the flow completed successfully."
