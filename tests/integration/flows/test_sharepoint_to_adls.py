from viadot.flows import SharepointToADLS
from unittest import mock
import pandas as pd
from prefect.tasks.secrets import PrefectSecret
import os


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
            adls_dir_path="raw/tests/test.csv",
        )
        result = flow.run()
        assert result.is_successful()
        os.remove("test_sharepoint_to_adls_run_flow.csv")
        os.remove("test_sharepoint_to_adls_run_flow.json")
