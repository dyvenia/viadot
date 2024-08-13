from viadot.orchestration.prefect.flows import hubspot_to_adls


def test_hubspot_to_adls():
    state = hubspot_to_adls(
        azure_key_vault_secret="hubspot-access",  # noqa: S106
        endpoint="hubdb/api/v2/tables/6009756/rows/draft",
        nrows=1000000,
        adls_azure_key_vault_secret="app-azure-cr-datalakegen2",  # noqa: S106
        adls_path="raw/dyvenia_sandbox/genesys/hubspot_rial.parquet",
        adls_path_overwrite=True,
    )
    assert state.is_successful()
