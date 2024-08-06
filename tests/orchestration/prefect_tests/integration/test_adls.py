import pandas as pd
import pytest
from prefect import flow, task
from viadot.orchestration.prefect.tasks import df_to_adls

try:
    from viadot.sources import AzureDataLake

    _adls_installed = True
except ImportError:
    _adls_installed = False

if not _adls_installed:
    pytest.skip("AzureDataLake source not installed", allow_module_level=True)


def test_df_to_adls(TEST_FILE_PATH):
    lake = AzureDataLake(config_key="adls_test")

    @task
    def create_df():
        return pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    @task
    def check_file_exists(path):
        return lake.exists(path)

    @flow
    def test_flow():
        df = create_df()
        to_adls = df_to_adls(
            df=df,
            path=TEST_FILE_PATH,
            config_key="adls_test",
            overwrite=True,
        )
        return check_file_exists(TEST_FILE_PATH, wait_for=to_adls)

    result = test_flow()
    assert result is True

    # Cleanup.
    lake.rm(TEST_FILE_PATH)
