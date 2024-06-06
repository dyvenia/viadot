import os
from pathlib import Path

from prefect import flow
from viadot.orchestration.prefect.tasks import (
    sharepoint_download_file,
    sharepoint_to_df,
)


def test_to_df(sharepoint_url, sharepoint_config_key):
    @flow
    def test_to_df_flow():
        return sharepoint_to_df(url=sharepoint_url, config_key=sharepoint_config_key)

    received_df = test_to_df_flow()
    assert not received_df.empty


def test_download_file(sharepoint_url, sharepoint_config_key):
    file = "sharepoint_test" + sharepoint_url.split(".")[-1]

    @flow
    def test_download_file_flow():
        return sharepoint_download_file(
            url=sharepoint_url, to_path=file, config_key=sharepoint_config_key
        )

    test_download_file_flow()

    assert file in os.listdir()

    Path(file).unlink()
