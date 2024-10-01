"""Test flows `test_eurostat_to_adls`."""

from viadot.orchestration.prefect.flows import (
    eurostat_to_adls,
)
from viadot.sources import AzureDataLake


TEST_FILE_PATH = "raw/viadot_2_0_TEST_eurostat.parquet"


def test_eurostat_to_adls():
    """Function for testing uploading data from Eurostat to ADLS."""
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_FILE_PATH)

    eurostat_to_adls(
        dataset_code="ILC_DI04",
        adls_path=TEST_FILE_PATH,
        adls_credentials_secret="sp-adls-test",  # noqa: S106, # pragma: allowlist secret
    )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)
