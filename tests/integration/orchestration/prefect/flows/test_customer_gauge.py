from viadot.orchestration.prefect.flows import (
    customer_gauge_to_adls,
)
from viadot.sources import AzureDataLake

TEST_SCHEMA = "test_viadot_schema"
TEST_TABLE = "test"


def test_customer_gauge_to_adls(
    endpoint,
    TEST_FILE_PATH,
    adls_credentials_secret,
):
    lake = AzureDataLake(config_key="adls_test")
    assert not lake.exists(TEST_FILE_PATH)

    customer_gauge_to_adls(
        endpoint=endpoint,
        adls_path=TEST_FILE_PATH,
        adls_credentials_secret=adls_credentials_secret,
    )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)
