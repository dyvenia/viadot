import os
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
import pytest


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
    return pd.DataFrame.from_dict(
        data={"country": ["italy", "germany", "spain"], "sales": [100, 50, 80]}
    )


@pytest.fixture(scope="session", autouse=True)
def _create_test_csv_file(DF, TEST_CSV_FILE_PATH):
    DF.to_csv(TEST_CSV_FILE_PATH, index=False, sep="\t")
    yield
    Path(TEST_CSV_FILE_PATH).unlink()


@pytest.fixture(scope="session", autouse=True)
def _create_test_parquet_file(DF, TEST_PARQUET_FILE_PATH):
    DF.to_parquet(TEST_PARQUET_FILE_PATH, index=False)
    yield
    Path(TEST_PARQUET_FILE_PATH).unlink()


@pytest.fixture(scope="session", autouse=True)
def _create_test_parquet_file_2(DF, TEST_PARQUET_FILE_PATH_2):
    DF.to_parquet(TEST_PARQUET_FILE_PATH_2, index=False)
    yield
    Path(TEST_PARQUET_FILE_PATH_2).unlink()


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
    return os.environ.get("VIADOT_TEST_REDSHIFT_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def s3_config_key():
    return os.environ.get("VIADOT_TEST_S3_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def sharepoint_config_key():
    return os.environ.get("VIADOT_TEST_SHAREPOINT_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def databricks_config_key():
    return os.environ.get("VIADOT_TEST_DATABRICKS_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def c4c_config_key():
    return os.environ.get("VIADOT_TEST_C4C_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def trino_config_key():
    return os.environ.get("VIADOT_TEST_TRINO_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def minio_config_key():
    return os.environ.get("VIADOT_TEST_MINIO_CONFIG_KEY")


# Prefect


@pytest.fixture(scope="session", autouse=True)
def TEST_FILE_PATH():
    return os.environ.get("TEST_FILE_PATH")


@pytest.fixture(scope="session", autouse=True)
def TEST_DF():
    return pd.DataFrame.from_dict(
        data={"country": ["italy", "germany", "spain"], "sales": [100, 50, 80]}
    )


@pytest.fixture(scope="session", autouse=True)
def AZURE_ORG_NAME():
    return os.environ.get("AZURE_ORG_NAME")


@pytest.fixture(scope="session", autouse=True)
def AZURE_PROJECT_NAME():
    return os.environ.get("AZURE_PROJECT_NAME")


@pytest.fixture(scope="session", autouse=True)
def AZURE_REPO_NAME():
    return os.environ.get("AZURE_REPO_NAME")


@pytest.fixture(scope="session", autouse=True)
def AZURE_REPO_URL():
    return os.environ.get("AZURE_REPO_URL")


@pytest.fixture(scope="session", autouse=True)
def sharepoint_credentials_secret():
    return os.environ.get("VIADOT_TEST_SHAREPOINT_CREDENTIALS_SECRET")


@pytest.fixture(scope="session", autouse=True)
def aws_config_key():
    return os.environ.get("VIADOT_TEST_AWS_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def outlook_config_key():
    return os.environ.get("VIADOT_TEST_OUTLOOK_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def mindful_config_key():
    return os.environ.get("VIADOT_TEST_MINDFUL_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def hubspot_config_key():
    return os.environ.get("VIADOT_TEST_HUBSPOT_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def LUMA_URL():
    return os.environ.get("LUMA_URL")


@pytest.fixture(scope="session", autouse=True)
def dbt_repo_url():
    return os.environ.get("DBT_REPO_URL")


@pytest.fixture(scope="session", autouse=True)
def exchange_rates_config_key():
    return os.environ.get("VIADOT_TEST_EXCHANGE_RATES_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def adls_credentials_secret():
    return os.environ.get("VIADOT_TEST_ADLS_CREDENTIALS_SECRET")


@pytest.fixture(scope="session", autouse=True)
def databricks_credentials_secret():
    return os.environ.get("VIADOT_TEST_DATABRICKS_CREDENTIALS_SECRET")


@pytest.fixture(scope="session", autouse=True)
def c4c_credentials_secret():
    return os.environ.get("VIADOT_TEST_C4C_CREDENTIALS_SECRET")


@pytest.fixture(scope="session", autouse=True)
def VIADOT_TEST_MEDIATOOL_ORG():
    return os.environ.get("VIADOT_TEST_MEDIATOOL_ORG")


@pytest.fixture(scope="session", autouse=True)
def VIADOT_TEST_MEDIATOOL_ADLS_AZURE_KEY_VAULT_SECRET():
    return os.environ.get("VIADOT_TEST_MEDIATOOL_ADLS_AZURE_KEY_VAULT_SECRET")


@pytest.fixture(scope="session", autouse=True)
def VIADOT_TEST_MEDIATOOL_ADLS_PATH():
    return os.environ.get("VIADOT_TEST_MEDIATOOL_ADLS_PATH")


@pytest.fixture(scope="session", autouse=True)
def VIADOT_TEST_ADLS_AZURE_KEY_VAULT_SECRET():
    return os.environ.get("VIADOT_TEST_ADLS_AZURE_KEY_VAULT_SECRET")
