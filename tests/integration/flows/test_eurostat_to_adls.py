import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import EurostatToADLS

DATA = {"geo": ["PL", "DE", "NL"], "indicator": [35, 55, 77]}
ADLS_FILE_NAME = "test_eurostat.parquet"
ADLS_DIR_PATH = "raw/tests/"


@mock.patch(
    "viadot.tasks.EurostatToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_eurostat_to_adls_run_flow(mocked_class):
    flow = EurostatToADLS(
        "test_eurostat_to_adls_flow_run",
        dataset_code="ILC_DI04",
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_eurostat_to_adls_flow_run.parquet")
    os.remove("test_eurostat_to_adls_flow_run.json")
