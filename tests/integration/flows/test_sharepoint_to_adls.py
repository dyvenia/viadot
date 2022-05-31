from viadot.flows import SharepointToADLS
from unittest import mock
import pandas as pd
from prefect.tasks.secrets import PrefectSecret
import os
import pendulum

FILE_NAME = str(pendulum.now("utc")) + ".csv"


def test_sharepoint_to_adls_run_flow():

    d = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=d)

    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
    ).run()

    with mock.patch(
        "viadot.flows.sharepoint_to_adls.excel_to_df_task.bind"
    ) as excel_to_df_task_mock:
        excel_to_df_task_mock.return_value = df

        flow = SharepointToADLS(
            "test_sharepoint_to_adls_run_flow",
            output_file_extension=".csv",
            adls_sp_credentials_secret=credentials_secret,
            adls_dir_path="raw/tests",
            adls_file_name=FILE_NAME,
        )
        result = flow.run()
        assert result.is_successful()
        os.remove("test_sharepoint_to_adls_run_flow.csv")
        os.remove("test_sharepoint_to_adls_run_flow.json")


def test_sharepoint_to_adls_run_flow_overwrite_true():

    d = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=d)

    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
    ).run()

    with mock.patch(
        "viadot.flows.sharepoint_to_adls.excel_to_df_task.bind"
    ) as excel_to_df_task_mock:
        excel_to_df_task_mock.return_value = df

        flow = SharepointToADLS(
            "test_sharepoint_to_adls_run_flow_overwrite_true",
            output_file_extension=".csv",
            adls_sp_credentials_secret=credentials_secret,
            adls_dir_path="raw/tests",
            adls_file_name=FILE_NAME,
            overwrite_adls=True,
        )
        result = flow.run()

        assert result.is_successful()
        os.remove("test_sharepoint_to_adls_run_flow_overwrite_true.csv")
        os.remove("test_sharepoint_to_adls_run_flow_overwrite_true.json")


def test_sharepoint_to_adls_run_flow_overwrite_false():

    d = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=d)

    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
    ).run()

    with mock.patch(
        "viadot.flows.sharepoint_to_adls.excel_to_df_task.bind"
    ) as excel_to_df_task_mock:
        excel_to_df_task_mock.return_value = df

        flow = SharepointToADLS(
            "test_sharepoint_to_adls_run_flow_overwrite_false",
            output_file_extension=".csv",
            adls_sp_credentials_secret=credentials_secret,
            adls_dir_path="raw/tests",
            adls_file_name=FILE_NAME,
            overwrite_adls=False,
        )
        result = flow.run()

        assert result.is_failed()
        os.remove("test_sharepoint_to_adls_run_flow_overwrite_false.csv")
        os.remove("test_sharepoint_to_adls_run_flow_overwrite_false.json")
