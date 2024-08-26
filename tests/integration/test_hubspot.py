from viadot.orchestration.prefect.flows import hubspot_to_adls


def test_hubspot_to_adls(hubspot_config_key, adls_credentials_secret):
    state = hubspot_to_adls(
        config_key=hubspot_config_key,
        endpoint="hubdb/api/v2/tables/6009756/rows/draft",
        nrows=1000000,
        adls_azure_key_vault_secret=adls_credentials_secret,
        adls_path="raw/dyvenia_sandbox/genesys/hubspot_rial.parquet",
        adls_path_overwrite=True,
    )
    assert state.is_successful()
