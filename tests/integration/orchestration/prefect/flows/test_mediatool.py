"""'test_mediatool.py'."""

from viadot.orchestration.prefect.flows import mediatool_to_adls


def test_genesys_to_adls(
    azure_key_vault_secret,
    organization_ids,
    media_entries_columns,
    adls_azure_key_vault_secret,
):
    state = mediatool_to_adls(
        azure_key_vault_secret=azure_key_vault_secret,
        organization_ids=organization_ids,
        media_entries_columns=media_entries_columns,
        adls_path="raw/dyvenia_sandbox/mediatool/mediatool_trial.parquet",
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_path_overwrite=True,
    )

    assert state.is_successful()
