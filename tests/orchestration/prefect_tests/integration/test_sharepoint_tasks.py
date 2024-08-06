import os
from pathlib import Path

import pandas as pd
import pytest
from prefect import flow
from viadot.orchestration.prefect.tasks import (
    get_endpoint_type_from_url,
    scan_sharepoint_folder,
    sharepoint_download_file,
    sharepoint_to_df,
    validate_and_reorder_dfs_columns,
)

DF1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})


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


def test_scan_sharepoint_folder(sharepoint_folder_url, sharepoint_config_key):
    @flow
    def test_scan_sharepoint_folder_flow():
        return scan_sharepoint_folder(
            url=sharepoint_folder_url, config_key=sharepoint_config_key
        )

    received_files = test_scan_sharepoint_folder_flow()
    assert received_files
    assert isinstance(received_files, list)
    assert all(isinstance(file_url, str) for file_url in received_files)


def test_get_endpoint_type_from_url():
    test_cases = [
        (
            "https://company.sharepoint.com/sites/site_name/shared_documents/folder",
            "directory",
        ),
        (
            "https://company.sharepoint.com/sites/site_name/shared_documents/",
            "directory",
        ),
        (
            "https://company.sharepoint.com/sites/site_name/shared_documents/folder/file.txt",
            "file",
        ),
        (
            "https://company.sharepoint.com/sites/site_name/shared_documents/folder/file.csv",
            "file",
        ),
    ]

    for url, expected_type in test_cases:
        assert get_endpoint_type_from_url(url) == expected_type


def test_validate_and_reorder_dfs_columns_empty_list():
    """Test case for empty list of dataframes."""
    with pytest.raises(IndexError):
        validate_and_reorder_dfs_columns([])


def test_validate_and_reorder_dfs_columns_different_structure():
    """Test case for different column structures."""
    df2 = pd.DataFrame({"a": [5, 6], "c": [7, 8]})
    with pytest.raises(
        ValueError,
        match=r"""does not have the same structure as
            the first DataFrame.""",
    ):
        validate_and_reorder_dfs_columns([DF1, df2])


def test_validate_and_reorder_dfs_columns_reorder_columns():
    """Test case for reordering columns."""
    df3 = pd.DataFrame({"b": [3, 4], "a": [1, 2]})
    df4 = pd.DataFrame({"a": [5, 6], "b": [7, 8]})
    result = validate_and_reorder_dfs_columns([DF1, df3, df4])
    for df in result:
        assert df.columns.tolist() == ["a", "b"]


def test_validate_and_reorder_dfs_columns_correct_order():
    """Test case for columns already in order."""
    df5 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = validate_and_reorder_dfs_columns([DF1, df5])
    for df in result:
        assert df.columns.tolist() == ["a", "b"]
