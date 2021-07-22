import json
import os
import uuid

from prefect.storage import Local

from viadot.flows import SupermetricsToAzureSQLv3

CWD = os.getcwd()
STORAGE = Local(path=CWD)

expectation_suite = {
    "data_asset_type": "Dataset",
    "expectation_suite_name": "failure",
    "expectations": [
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"max_value": 10, "min_value": 10},
            "meta": {},
        },
    ],
    "meta": {
        "columns": {
            "All Users": {"description": ""},
            "Date": {"description": ""},
            "M-Site_Better Space: All Landing Page Sessions": {"description": ""},
            "M-site_Accessories: All Landing Page Sessions": {"description": ""},
            "M-site_More Space: All Landing Page Sessions": {"description": ""},
            "M-site_Replacement: All Landing Page Sessions": {"description": ""},
        },
        "great_expectations_version": "0.13.19",
    },
}

with open(os.path.join(CWD, "expectations", "failure.json"), "w") as f:
    json.dump(expectation_suite, f)

uuid_4 = uuid.uuid4()
file_name = f"test_file_{uuid_4}.csv"
adls_path = f"raw/supermetrics/{file_name}"


def test_supermetrics_to_azure_sql():
    flow = SupermetricsToAzureSQLv3(
        "Google Analytics Load Times extract test",
        ds_id="GA",
        ds_segments=[
            "R1fbzFNQQ3q_GYvdpRr42w",
            "I8lnFFvdSFKc50lP7mBKNA",
            "Lg7jR0VWS5OqGPARtGYKrw",
            "h8ViuGLfRX-cCL4XKk6yfQ",
            "-1",
        ],
        ds_accounts=["8326007", "58338899"],
        date_range_type="last_year_inc",
        fields=[
            {"id": "Date"},
            {"id": "segment", "split": "column"},
            {"id": "AvgPageLoadTime_calc"},
        ],
        settings={"avoid_sampling": "true"},
        order_columns="alphabetic",
        max_columns=100,
        max_rows=10,
        expectation_suite_name="failure",
        adls_path=adls_path,
        dtypes={
            "Date": "DATE",
            "All Users": "FLOAT(24)",
            "M-Site_Better Space: All Landing Page Sessions": "VARCHAR(255)",
            "M-site_Accessories: All Landing Page Sessions": "VARCHAR(255)",
            "M-site_More Space: All Landing Page Sessions": "FLOAT(24)",
            "M-site_Replacement: All Landing Page Sessions": "VARCHAR(255)",
        },
        schema="sandbox",
        table="test_supermetrics_to_azure_sql",
        parallel=False,
        storage=STORAGE,
    )
    result = flow.run()
    assert result.is_successful

    task_results = result.result.values()
    assert all([task_result.is_successful for task_result in task_results])
