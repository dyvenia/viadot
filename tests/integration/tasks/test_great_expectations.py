import json
import os
import shutil

import pytest
from prefect.engine import signals

from viadot.tasks import RunGreatExpectationsValidation

CWD = os.getcwd()
GE_PROJECT_PATH = os.path.join(CWD, "expectations_test")
EXPECTATIONS_PATH = os.path.join(GE_PROJECT_PATH, "expectations")


@pytest.fixture(scope="function")
def expectation_suite_pass():

    expectation_suite = {
        "data_asset_type": "Dataset",
        "expectation_suite_name": "failure",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"max_value": 3, "min_value": 3},
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

    if not os.path.exists(EXPECTATIONS_PATH):
        os.makedirs(EXPECTATIONS_PATH)

    expectation_suite_path = os.path.join(EXPECTATIONS_PATH, "failure.json")

    with open(expectation_suite_path, "w") as f:
        json.dump(expectation_suite, f)

    yield

    shutil.rmtree(GE_PROJECT_PATH)


@pytest.fixture(scope="function")
def expectation_suite_fail():

    expectation_suite = {
        "data_asset_type": "Dataset",
        "expectation_suite_name": "failure",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"max_value": 3, "min_value": 3},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "meta": {},
                "kwargs": {"column": "sales", "min_value": 10, "max_value": 50},
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

    if not os.path.exists(EXPECTATIONS_PATH):
        os.makedirs(EXPECTATIONS_PATH)

    expectation_suite_path = os.path.join(EXPECTATIONS_PATH, "failure.json")

    with open(expectation_suite_path, "w") as f:
        json.dump(expectation_suite, f)

    yield

    shutil.rmtree(GE_PROJECT_PATH)


def test_great_expectations_pass(expectation_suite_pass, DF):

    great_expectations_task = RunGreatExpectationsValidation()

    try:
        great_expectations_task.run(
            df=DF, expectations_path=EXPECTATIONS_PATH, expectation_suite_name="failure"
        )
    except signals.FAIL:
        pytest.fail("Expectation run failed.")


def test_great_expectations_fail(expectation_suite_fail, DF):

    great_expectations_task = RunGreatExpectationsValidation()

    with pytest.raises(signals.FAIL):
        great_expectations_task.run(
            df=DF, expectations_path=EXPECTATIONS_PATH, expectation_suite_name="failure"
        )
