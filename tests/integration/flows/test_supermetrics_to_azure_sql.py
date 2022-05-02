import os
import pandas as pd
from unittest import mock
from viadot.flows import SupermetricsToAzureSQL
from io import StringIO
from csv import reader


def test_supermetrics_to_azure_sql_init():

    tasks_set = """{<Task: Constant[str]>,
                <Task: blob_to_azure_sql>,
                <Task: csv_to_blob_storage>,
                <Task: supermetrics_to_csv>}"""

    instance = SupermetricsToAzureSQL(
        "test_name",
        ds_id="example_id",
        ds_accounts="example_accounts",
        ds_user="example_user",
        fields=["filed", "field2"],
    )

    assert instance
    assert instance.__dict__["tasks"] == tasks_set


def test_supermetrics_to_azure_sql_run_flow():

    with mock.patch.object(
        SupermetricsToAzureSQL, "run", return_value=True
    ) as mock_method:
        flow = SupermetricsToAzureSQL(
            "test_name_extract",
            ds_id="example_id",
            ds_accounts="example_accounts",
            ds_user="example_user",
            date_range_type="last_year_inc",
            max_rows=10,
            fields=["Date", "profile", "Campaignname"],
            schema="raw",
            table="test_name_extract",
            local_file_path="test.csv",
            blob_path="tests/supermetrics/test.csv",
            if_exists="replace",
            dtypes={
                "Date": "DATE",
                "profile": "VARCHAR(255)",
                "Campaignname": "VARCHAR(255)",
            },
        )

        flow.run()
        mock_method.assert_called_with()
