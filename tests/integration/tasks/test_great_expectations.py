import json
import os
import shutil

import pytest
from prefect.engine import signals
from viadot.tasks import RunGreatExpectationsValidation

CWD = os.getcwd()
PATH = os.path.join(CWD, "expectations_test")


@pytest.fixture(scope="function")
def expectation_suite():

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

    if not os.path.exists(os.path.join(PATH, "expectations")):
        os.makedirs(os.path.join(PATH, "expectations"))

    expectation_suite_path = os.path.join(PATH, "expectations", "failure.json")

    with open(expectation_suite_path, "w") as f:
        json.dump(expectation_suite, f)

    yield

    shutil.rmtree(PATH)


def test_great_expectations(expectation_suite, DF):
    great_expectations_task = RunGreatExpectationsValidation()
    try:
        great_expectations_task.run(
            df=DF, expectations_path=PATH, expectation_suite_name="failure"
        )
    except signals.FAIL:
        pytest.fail("Expectation run failed.")
