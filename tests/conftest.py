import os

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def TEST_SUPERMETRICS_FILE_PATH():
    return "test_supermetrics.csv"


@pytest.fixture(scope="session")
def TEST_CSV_FILE_PATH():
    return "test_data_countries.csv"


@pytest.fixture(scope="session")
def TEST_PARQUET_FILE_PATH():
    return "test_data_countries.parquet"


@pytest.fixture(scope="session")
def TEST_CSV_FILE_BLOB_PATH():
    return "testing/testing_access/test.csv"


@pytest.fixture(scope="session", autouse=True)
def DF():
    df = pd.DataFrame.from_dict(
        data={"country": ["italy", "germany", "spain"], "sales": [100, 50, 80]}
    )
    return df


@pytest.fixture(scope="session", autouse=True)
def create_test_csv_file(DF, TEST_CSV_FILE_PATH):
    DF.to_csv(TEST_CSV_FILE_PATH, index=False, sep="\t")
    yield
    os.remove(TEST_CSV_FILE_PATH)


@pytest.fixture(scope="session", autouse=True)
def create_test_parquet_file(DF, TEST_PARQUET_FILE_PATH):
    DF.to_parquet(TEST_PARQUET_FILE_PATH, index=False)
    yield
    os.remove(TEST_PARQUET_FILE_PATH)
