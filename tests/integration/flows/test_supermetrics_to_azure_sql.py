import os
from csv import reader
from io import StringIO
from unittest import mock

import pandas as pd
from prefect.storage import Local

from viadot.config import local_config
from viadot.flows import SupermetricsToAzureSQL

CWD = os.getcwd()
adls_dir_path = "raw/supermetrics"
STORAGE = Local(path=CWD)
SCHEMA = "sandbox"
TABLE = "test_supermetrics"


def test_supermetrics_to_azure_sql_init():

    instance = SupermetricsToAzureSQL(
        "test_name",
        ds_id="example_id",
        ds_accounts="example_accounts",
        ds_user="example_user",
        fields=["filed", "field2"],
    )

    assert instance
    assert instance.__dict__["ds_id"] == "example_id"


def test_supermetrics_to_azure_sql_run_flow_mock():

    with mock.patch.object(
        SupermetricsToAzureSQL, "run", return_value=True
    ) as mock_method:

        credentials = local_config.get("SUPERMETRICS")

        flow = SupermetricsToAzureSQL(
            "test_supermetrics",
            ds_id="GA",
            ds_segments=[
                "R1fbzFNQQ3q_GYvdpRr42w",
                "I8lnFFvdSFKc50lP7mBKNA",
                "Lg7jR0VWS5OqGPARtGYKrw",
                "h8ViuGLfRX-cCL4XKk6yfQ",
                "-1",
            ],
            ds_accounts=["8326007", "58338899"],
            date_range_type="last_month",
            ds_user=credentials["USER"],
            fields=[
                {"id": "Date"},
                {"id": "segment", "split": "column"},
                {"id": "AvgPageLoadTime_calc"},
            ],
            dtypes={
                "date": "DATE",
                "segment": "VARCHAR(255)",
                "AvgPageLoadTime_calc": "VARCHAR(255)",
            },
            settings={"avoid_sampling": "true"},
            order_columns="alphabetic",
            max_columns=10,
            max_rows=1,
            schema=SCHEMA,
            table=TABLE,
            local_file_path="test_supermetrics.csv",
            blob_path="tests/test.csv",
            storage=STORAGE,
        )

        flow.run()
        mock_method.assert_called_with()
