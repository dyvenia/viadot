import json
import os
import shutil

import pytest
from prefect.engine import signals

from viadot.tasks import RunGreatExpectationsValidation

CWD = os.getcwd()
PATH = os.path.join(CWD, "expectations_test")
EXPECTATIONS_PATH = os.path.join(PATH, "expectations")


@pytest.fixture(scope="function")
def expectation_suite_pass():

    expectation_suite = {
        "data_asset_type": "Dataset",
        "expectation_suite_name": "failure",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": {"$PARAMETER": "trunc(previous_run_row_count * 0.9)"},
                    "max_value": {"$PARAMETER": "trunc(previous_run_row_count * 1.1)"},
                    "meta": {},
                },
            }
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
        "evaluation_parameters": {
            "previous_run_row_count": 3,
        },
    }

    if not os.path.exists(EXPECTATIONS_PATH):
        os.makedirs(EXPECTATIONS_PATH)

    expectation_suite_path = os.path.join(EXPECTATIONS_PATH, "failure.json")

    with open(expectation_suite_path, "w") as f:
        json.dump(expectation_suite, f)

    yield

    shutil.rmtree(PATH)


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
        "evaluation_parameters": {
            "previous_run_row_count": 3,
        },
    }

    if not os.path.exists(EXPECTATIONS_PATH):
        os.makedirs(EXPECTATIONS_PATH)

    expectation_suite_path = os.path.join(EXPECTATIONS_PATH, "failure.json")

    with open(expectation_suite_path, "w") as f:
        json.dump(expectation_suite, f)

    yield

    shutil.rmtree(PATH)


def test_ge_pass(expectation_suite_pass, DF):

    great_expectations_task = RunGreatExpectationsValidation()

    try:
        great_expectations_task.run(
            df=DF,
            expectations_path=EXPECTATIONS_PATH,
            expectation_suite_name="failure",
        )
    except signals.FAIL:
        pytest.fail("Expectation run failed.")


def test_ge_fail(expectation_suite_fail, DF):

    great_expectations_task = RunGreatExpectationsValidation()

    with pytest.raises(signals.FAIL):
        great_expectations_task.run(
            df=DF, expectations_path=EXPECTATIONS_PATH, expectation_suite_name="failure"
        )


def test_ge_evaluation_params(expectation_suite_pass, DF):
    great_expectations_task = RunGreatExpectationsValidation()

    evaluation_params = dict(previous_run_row_count=3)

    try:
        great_expectations_task.run(
            df=DF,
            expectations_path=EXPECTATIONS_PATH,
            expectation_suite_name="failure",
            evaluation_parameters=evaluation_params,
        )
    except signals.FAIL:
        pytest.fail("Expectation run failed.")

    failing_evaluation_params = dict(previous_run_row_count=2)
    with pytest.raises(signals.FAIL):
        great_expectations_task.run(
            df=DF,
            expectations_path=EXPECTATIONS_PATH,
            expectation_suite_name="failure",
            evaluation_parameters=failing_evaluation_params,
        )
