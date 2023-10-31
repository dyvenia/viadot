import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import MediatoolToADLS
from viadot.exceptions import ValidationError

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
    assert len(flow.tasks) == 10
    os.remove("test_mediatool_to_adls_flow_run.parquet")
    os.remove("test_mediatool_to_adls_flow_run.json")


@mock.patch(
    "viadot.tasks.MediatoolToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_mediatool_to_adls_run_flow_validate_fail(mocked_class):
    flow = MediatoolToADLS(
        "test_mediatool_to_adls_flow_run",
        organization_ids=["1000000001", "200000001"],
        media_entries_columns=["id", "name", "num"],
        mediatool_credentials_key="MEDIATOOL-TESTS",
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        validate_df_dict={"column_size": {"country": 10}},
    )
    try:
        flow.run()
    except ValidationError:
        pass


@mock.patch(
    "viadot.tasks.MediatoolToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_mediatool_to_adls_run_flow_validate_success(mocked_class):
    flow = MediatoolToADLS(
        "test_mediatool_to_adls_flow_run",
        organization_ids=["1000000001", "200000001"],
        media_entries_columns=["id", "name", "num"],
        mediatool_credentials_key="MEDIATOOL-TESTS",
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        validate_df_dict={"column_size": {"country": 2}},
    )
    result = flow.run()
    assert result.is_successful()
    assert len(flow.tasks) == 11
    os.remove("test_mediatool_to_adls_flow_run.parquet")
    os.remove("test_mediatool_to_adls_flow_run.json")
