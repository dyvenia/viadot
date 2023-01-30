import os
from unittest import mock
import pytest
import pandas as pd
import pendulum
from prefect.tasks.secrets import PrefectSecret

from viadot.flows import SharepointToADLS
from viadot.tasks import AzureDataLakeRemove

ADLS_FILE_NAME = str(pendulum.now("utc")) + ".csv"
ADLS_DIR_PATH = "raw/tests/"
CREDENTIALS_SECRET = PrefectSecret("AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET").run()
DATA = {"country": [1, 2], "sales": [3, 4]}


@mock.patch(
    "viadot.tasks.SharepointToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sharepoint_to_adls_run_flow(mocked_class):
    flow = SharepointToADLS(
        "test_sharepoint_to_adls_run_flow",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_sharepoint_to_adls_run_flow.csv")
    os.remove("test_sharepoint_to_adls_run_flow.json")


@mock.patch(
    "viadot.tasks.SharepointToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sharepoint_to_adls_run_flow_overwrite_true(mocked_class):
    flow = SharepointToADLS(
        "test_sharepoint_to_adls_run_flow_overwrite_true",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        overwrite_adls=True,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_sharepoint_to_adls_run_flow_overwrite_true.csv")
    os.remove("test_sharepoint_to_adls_run_flow_overwrite_true.json")


@mock.patch(
    "viadot.tasks.SharepointToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sharepoint_to_adls_run_flow_overwrite_false(mocked_class):
    flow = SharepointToADLS(
        "test_sharepoint_to_adls_run_flow_overwrite_false",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        overwrite_adls=False,
    )
    result = flow.run()

    assert result.is_failed()
    os.remove("test_sharepoint_to_adls_run_flow_overwrite_false.csv")
    os.remove("test_sharepoint_to_adls_run_flow_overwrite_false.json")
