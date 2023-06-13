import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import MediatoolToADLS

DATA = {"country": ["DK", "DE"], "sales": [3, 4]}
ADLS_FILE_NAME = "test_mediatool.parquet"
ADLS_DIR_PATH = "raw/tests/"


@mock.patch(
    "viadot.tasks.MediatoolToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_mediatool_to_adls_run_flow(mocked_class):
    flow = MediatoolToADLS(
        "test_mediatool_to_adls_flow_run",
        organization_ids=["1000000001", "200000001"],
        media_entries_columns=["id", "name", "num"],
        mediatool_credentials_key="MEDIATOOL-TESTS",
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_mediatool_to_adls_flow_run.parquet")
    os.remove("test_mediatool_to_adls_flow_run.json")
