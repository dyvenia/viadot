import pandas as pd
from prefect import flow, task

from viadot.orchestration.prefect.tasks import df_to_adls
from viadot.utils import skip_test_on_missing_extra


try:
    from viadot.sources import AzureDataLake
except ImportError:
    skip_test_on_missing_extra(source_name="AzureDataLake", extra="azure")


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
