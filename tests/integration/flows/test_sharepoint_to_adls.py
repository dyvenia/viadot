import os
from unittest import mock

import pandas as pd
import pendulum
import pytest
from prefect.tasks.secrets import PrefectSecret
from viadot.flows import SharepointToADLS, SharepointListToADLS
from viadot.tasks import AzureDataLakeRemove

ADLS_FILE_NAME = str(pendulum.now("utc")) + ".csv"
ADLS_FILE_NAME_LIST = pendulum.now("utc").strftime("%Y-%m-%d_%H:%M:%S_%Z%z")
ADLS_DIR_PATH = "raw/tests/"
CREDENTIALS_SECRET = PrefectSecret("AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET").run()
DATA = {"country": [1, 2], "sales": [3, 4]}
EMPTY_DATA = {}


# SharepointToADLS


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


# SharepointListToADLS


@mock.patch(
    "viadot.tasks.SharepointListToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sharepoint_list_to_adls_run_flow_csv(mocked_class):
    flow = SharepointListToADLS(
        "test_sharepoint_to_adls_run_flow",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        file_name=ADLS_FILE_NAME_LIST,
        list_title="",
        site_url="",
    )
    result = flow.run()
    assert result.is_successful()
    os.remove(ADLS_FILE_NAME_LIST + ".csv")
    os.remove("test_sharepoint_to_adls_run_flow.json")


@mock.patch(
    "viadot.tasks.SharepointListToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sharepoint_list_to_adls_run_flow_parquet(mocked_class):
    flow = SharepointListToADLS(
        "test_sharepoint_to_adls_run_flow",
        output_file_extension=".parquet",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        file_name=ADLS_FILE_NAME_LIST,
        list_title="",
        site_url="",
    )
    result = flow.run()
    assert result.is_successful()
    os.remove(ADLS_FILE_NAME_LIST + ".parquet")
    os.remove("test_sharepoint_to_adls_run_flow.json")


@mock.patch(
    "viadot.tasks.SharepointListToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sharepoint_list_to_adls_run_flow_wrong_extension(mocked_class):
    with pytest.raises(ValueError) as exc:
        flow = SharepointListToADLS(
            "test_sharepoint_to_adls_run_flow",
            output_file_extension=".s",
            adls_sp_credentials_secret=CREDENTIALS_SECRET,
            adls_dir_path=ADLS_DIR_PATH,
            file_name=ADLS_FILE_NAME_LIST,
            list_title="",
            site_url="",
        )
        result = flow.run()
    assert "Output file extension can only be '.csv' or '.parquet'" in str(exc.value)


@mock.patch(
    "viadot.tasks.SharepointListToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_sharepoint_list_to_adls_run_flow_overwrite_true(mocked_class):
    flow = SharepointListToADLS(
        "test_sharepoint_to_adls_run_flow_overwrite_true",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        file_name=ADLS_FILE_NAME_LIST,
        overwrite_adls=True,
        list_title="",
        site_url="",
    )
    result = flow.run()
    assert result.is_successful()
    os.remove(ADLS_FILE_NAME_LIST + ".csv")
    os.remove("test_sharepoint_to_adls_run_flow_overwrite_true.json")


@mock.patch(
    "viadot.tasks.SharepointListToDF.run",
    return_value=pd.DataFrame(data=EMPTY_DATA),
)
@pytest.mark.run
def test_sharepoint_list_to_adls_run_flow_fail_on_no_data_returned(mocked_class):
    flow = SharepointListToADLS(
        "test_sharepoint_to_adls_run_flow",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        file_name=ADLS_FILE_NAME_LIST,
        list_title="",
        site_url="",
        if_no_data_returned="fail",
    )
    result = flow.run()
    assert result.is_failed()
    os.remove(ADLS_FILE_NAME_LIST + ".csv")
    os.remove("test_sharepoint_to_adls_run_flow.json")


@mock.patch(
    "viadot.tasks.SharepointListToDF.run",
    return_value=pd.DataFrame(data=EMPTY_DATA),
)
@pytest.mark.run
def test_sharepoint_list_to_adls_run_flow_success_on_no_data_returned(mocked_class):
    flow = SharepointListToADLS(
        "test_sharepoint_to_adls_run_flow",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        file_name=ADLS_FILE_NAME_LIST,
        list_title="",
        site_url="",
        if_no_data_returned="skip",
    )
    result = flow.run()
    assert result.is_successful()
    os.remove(ADLS_FILE_NAME_LIST + ".csv")
    os.remove("test_sharepoint_to_adls_run_flow.json")


@mock.patch(
    "viadot.tasks.SharepointListToDF.run",
    return_value=pd.DataFrame(data=EMPTY_DATA),
)
@pytest.mark.run
def test_sharepoint_list_to_adls_run_flow_success_warn_on_no_data_returned(
    mocked_class,
):
    # Get prefect client instance
    flow = SharepointListToADLS(
        "test_sharepoint_to_adls_run_flow",
        output_file_extension=".csv",
        adls_sp_credentials_secret=CREDENTIALS_SECRET,
        adls_dir_path=ADLS_DIR_PATH,
        file_name=ADLS_FILE_NAME_LIST,
        list_title="",
        site_url="",
        if_no_data_returned="warn",
    )
    result = flow.run()
    assert result.is_successful()
    os.remove(ADLS_FILE_NAME_LIST + ".csv")
    os.remove("test_sharepoint_to_adls_run_flow.json")
