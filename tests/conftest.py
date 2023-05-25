import os

import pandas as pd
import pytest
from dotenv import load_dotenv

load_dotenv()


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
def TEST_PARQUET_FILE_PATH_2():
    return "test_data_countries_2.parquet"


@pytest.fixture(scope="session")
def TEST_CSV_FILE_BLOB_PATH():
    return "tests/test.csv"


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


@pytest.fixture(scope="session", autouse=True)
def create_test_parquet_file_2(DF, TEST_PARQUET_FILE_PATH_2):
    DF.to_parquet(TEST_PARQUET_FILE_PATH_2, index=False)
    yield
    os.remove(TEST_PARQUET_FILE_PATH_2)


@pytest.fixture(scope="session", autouse=True)
def sharepoint_url():
    return os.environ.get("VIADOT_SHAREPOINT_URL")


@pytest.fixture(scope="session", autouse=True)
def TEST_ADLS_FILE_PATH_PARQUET():
    return os.environ.get("TEST_ADLS_FILE_PATH_PARQUET")


@pytest.fixture(scope="session", autouse=True)
def TEST_ADLS_FILE_PATH_CSV():
    return os.environ.get("TEST_ADLS_FILE_PATH_CSV")


@pytest.fixture(scope="session", autouse=True)
def redshift_config_key():
    return os.environ.get("VIADOT_REDSHIFT_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def s3_config_key():
    return os.environ.get("VIADOT_S3_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def sharepoint_config_key():
    return os.environ.get("VIADOT_SHAREPOINT_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def databricks_config_key():
    return os.environ.get("VIADOT_DATABRICKS_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def c4c_config_key():
    return os.environ.get("VIADOT_C4C_CONFIG_KEY")
