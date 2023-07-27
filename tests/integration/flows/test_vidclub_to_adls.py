import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import VidClubToADLS

DATA = {"col1": ["aaa", "bbb", "ccc"], "col2": [11, 22, 33]}
ADLS_FILE_NAME = "test_vid_club.parquet"
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
