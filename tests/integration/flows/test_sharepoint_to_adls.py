import os
from unittest import mock

import pandas as pd
import pendulum
from prefect.tasks.secrets import PrefectSecret

from viadot.flows import SharepointToADLS
from viadot.tasks import AzureDataLakeRemove

ADLS_FILE_NAME = str(pendulum.now("utc")) + ".csv"
ADLS_DIR_PATH = "raw/tests/"
CREDENTIALS_SECRET = PrefectSecret("AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET").run()


def test_sharepoint_to_adls_run_flow():

    d = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=d)

    with mock.patch(
        "viadot.flows.sharepoint_to_adls.excel_to_df_task.bind"
    ) as excel_to_df_task_mock:
        excel_to_df_task_mock.return_value = df

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


def test_sharepoint_to_adls_run_flow_overwrite_true():

    d = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=d)

    with mock.patch(
        "viadot.flows.sharepoint_to_adls.excel_to_df_task.bind"
    ) as excel_to_df_task_mock:
        excel_to_df_task_mock.return_value = df

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


def test_sharepoint_to_adls_run_flow_overwrite_false():

    d = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=d)

    with mock.patch(
        "viadot.flows.sharepoint_to_adls.excel_to_df_task.bind"
    ) as excel_to_df_task_mock:
        excel_to_df_task_mock.return_value = df

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

    rm = AzureDataLakeRemove(
        path=ADLS_DIR_PATH + ADLS_FILE_NAME, vault_name="azuwevelcrkeyv001s"
    )
    rm.run(sp_credentials_secret=CREDENTIALS_SECRET)
