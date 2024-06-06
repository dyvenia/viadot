import os

from dotenv import load_dotenv
import pandas as pd
import pytest


load_dotenv()


@pytest.fixture(scope="session", autouse=True)
def TEST_FILE_PATH():
    return os.environ.get("TEST_FILE_PATH")


@pytest.fixture(scope="session", autouse=True)
def sharepoint_url():
    return os.environ.get("SHAREPOINT_URL")


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
def sharepoint_config_key():
    return os.environ.get("VIADOT_TEST_SHAREPOINT_CONFIG_KEY")


@pytest.fixture(scope="session", autouse=True)
def sharepoint_credentials_secret():
    return os.environ.get("VIADOT_TEST_SHAREPOINT_CREDENTIALS_SECRET")


@pytest.fixture(scope="session", autouse=True)
def aws_config_key():
    return os.environ.get("AWS_CONFIG_KEY")


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
