import os
from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import ValidationError
from viadot.flows import VidClubToADLS

DATA = {"col1": ["aaa", "bbb", "ccc"], "col2": [11, 22, 33]}
ADLS_FILE_NAME = "test_vid_club.parquet"
ADLS_FILE_NAME2 = "test_vid_club_validate_df.parquet"
ADLS_DIR_PATH = "raw/test/"


@mock.patch(
    "viadot.tasks.VidClubToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_vidclub_to_adls_run_flow(mocked_class):
    flow = VidClubToADLS(
        "test_vidclub_to_adls_flow_run",
        source=["test"],
        from_date="2023-06-05",
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_vidclub_to_adls_flow_run.parquet")
    os.remove("test_vidclub_to_adls_flow_run.json")


def test_vidclub_validate_df_task_success(caplog):
    flow = VidClubToADLS(
        "test_vidclub_validate_df_task_success",
        source="product",
        cols_to_drop=[
            "submissionProductID",
            "submissionProductDate",
            "brand",
            "productCode",
        ],
        from_date="2023-10-25",
        to_date="2023-10-25",
        adls_dir_path="raw/tests",
        adls_file_name="test.parquet",
        adls_sp_credentials_secret="App-Azure-CR-DatalakeGen2-AIA",
        overwrite_adls=True,
        validate_df_dict={
            "column_size": {"submissionID": 5},
            "column_list_to_match": [
                "submissionID",
                "regionID",
                "productQuantity",
                "unit",
            ],
        },
    )

    result = flow.run()
    assert result.is_successful()


def test_vidclub_validate_df_task_fail(caplog):
    flow = VidClubToADLS(
        "test_vidclub_validate_df_task_fail",
        source="product",
        cols_to_drop=[
            "submissionProductID",
            "submissionProductDate",
            "brand",
            "productCode",
        ],
        from_date="2023-10-25",
        to_date="2023-10-25",
        adls_dir_path="raw/tests",
        adls_file_name="test.parquet",
        adls_sp_credentials_secret="App-Azure-CR-DatalakeGen2-AIA",
        overwrite_adls=True,
        validate_df_dict={
            "column_size": {"submissionID": 5},
            "column_unique_values": ["id"],
        },
    )

    result = flow.run()
    assert result.is_failed()
