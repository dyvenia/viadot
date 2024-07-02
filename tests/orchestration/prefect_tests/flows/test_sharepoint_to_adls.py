from viadot.orchestration.prefect.flows import sharepoint_to_adls
from viadot.sources import AzureDataLake


def test_sharepoint_to_adls(
    sharepoint_url,
    TEST_FILE_PATH,
    adls_credentials_secret,
    sharepoint_credentials_secret,
):
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_FILE_PATH)

    sharepoint_to_adls(
        sharepoint_url=sharepoint_url,
        adls_path=TEST_FILE_PATH,
        columns="A:B",
        sharepoint_credentials_secret=sharepoint_credentials_secret,
        adls_credentials_secret=adls_credentials_secret,
    )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)
