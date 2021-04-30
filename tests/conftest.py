import os

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def TEST_SUPERMETRICS_FILE_PATH():
    return "/home/viadot/tests/integration/test_supermetrics.csv"


@pytest.fixture(scope="session")
def TEST_CSV_FILE_PATH():
    return "/home/viadot/tests/test_data_countries.csv"


@pytest.fixture(scope="session")
def TEST_CSV_FILE_BLOB_PATH():
    return "testing/testing_access/test.csv"


@pytest.fixture(scope="session", autouse=True)
def create_test_csv_file(TEST_CSV_FILE_PATH):
    df = pd.DataFrame.from_dict(
        data={"country": ["italy", "germany", "spain"], "sales": [100, 50, 80]}
    )
    df.to_csv(TEST_CSV_FILE_PATH, index=False)
    yield
    os.remove(TEST_CSV_FILE_PATH)
