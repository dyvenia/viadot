import logging
import os

import pytest
from prefect.storage import Local
from viadot.flows import SupermetricsToADLS

CWD = os.getcwd()
adls_dir_path = "raw/supermetrics"
STORAGE = Local(path=CWD)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def expectation_suite():
    expectation_suite = {
        "data_asset_type": "Dataset",
        "expectation_suite_name": "failure",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "max_value": {"$PARAMETER": "trunc(previous_run_row_count * 1.2)"},
                    "min_value": {"$PARAMETER": "trunc(previous_run_row_count * 0.8)"},
                },
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

    yield expectation_suite


def test_supermetrics_to_adls(expectation_suite):
    flow = SupermetricsToADLS(
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
        expectation_suite=expectation_suite,
        evaluation_parameters=dict(previous_run_row_count=9),
        adls_dir_path=adls_dir_path,
        parallel=False,
        storage=STORAGE,
    )
    result = flow.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])
