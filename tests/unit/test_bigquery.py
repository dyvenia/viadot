import pandas as pd
import pandas_gbq
import pytest

from viadot.sources import BigQuery
from viadot.exceptions import CredentialError, DBDataAccessError


@pytest.fixture(scope="function")
def BIGQ():
    BQ = BigQuery(config_key="bigquery")
    yield BQ

@pytest.fixture(scope="session")
def BIGQ_tab():
    table_id1 = "manigeo.manigeo_tab"
    table_id2 = "manigeo.space"
    df = pd.DataFrame({"my_value": ["val1", "val2", "val3"]})
    pandas_gbq.to_gbq(df, table_id1, if_exists="replace")
    pandas_gbq.to_gbq(df, table_id2, if_exists="replace")


QUERY_PIVOTED = """
        SELECT name, SUM(number) AS total
           FROM `bigquery-public-data.usa_names.usa_1910_2013`
        GROUP BY name, gender
        ORDER BY total DESC 
        LIMIT 4
        """

RESPONSE_PIVOTED = {
    "name": {0: "James", 1: "John", 2: "Robert", 3: "Michael"},
    "total": {0: 4924235, 1: 4818746, 2: 4703680, 3: 4280040},
}


def test_credentials_missing():
    # Testing error handling for missing credentials.
    with pytest.raises(CredentialError, match=r"Credentials not found."):
        BigQuery(config_key="bigquery_missing")


def test_credentials_incorrect():
    # Testing error handling for incorrect credentials.
    with pytest.raises(CredentialError, 
                    match=r"""'type', 'project_id', 'private_key_id', 'private_key',
                    'client_email', 'client_id', 'auth_uri', 'token_uri',
                    'auth_provider_x509_cert_url', 'client_x509_cert_url'
                    credentials are required."""):
        BigQuery(config_key="bigquery_incorrect")


def test_query_is_df(BIGQ):
    # Checking if the output of the function to_df() is pd.DataFrame object
    df = BIGQ.to_df(QUERY_PIVOTED)
    assert isinstance(df, pd.DataFrame)


def test_list_project(BIGQ):
    # Testing function which returns project_id from credentials.
    project = BIGQ.get_project_id()
    assert project == "manifest-geode-341308"


def test_list_datasets(BIGQ):
    # Testing function which returns datasets from project
    datasets = list(BIGQ.list_datasets())
    assert datasets == ["manigeo", "official_empty"]


def test_list_tables(BIGQ):
    # Testing function which returns tables name from project
    datasets = BIGQ.list_datasets()
    tables = list(BIGQ.list_tables(datasets[0]))
    assert "space" and "manigeo_tab" in tables


def test_list_columns(BIGQ):
    # Testing function which returns columns names from project table
    datasets = BIGQ.list_datasets()
    tables = list(BIGQ.list_tables(datasets[0]))
    col_name = BIGQ.list_columns(datasets[0], tables[0])
    assert col_name == ["my_value"]


def test_to_df(BIGQ):
    # Testing function which returns pd.DataFrame from to_df() function 
    datasets = BIGQ.list_datasets()
    tables = list(BIGQ.list_tables(datasets[0]))
    df_expected = BIGQ.to_df(dataset_name=datasets[0], table_name=tables[0])
    assert df_expected['my_value'].to_dict() == {0: 'val1', 1: 'val2', 2: 'val3'}


def test_decorator_to_df(BIGQ):
    # Testing decorator in function which returns additional columns  
    datasets = BIGQ.list_datasets()
    tables = list(BIGQ.list_tables(datasets[0]))
    df_col_expected = BIGQ.to_df(dataset_name=datasets[0], table_name=tables[0])
    assert df_col_expected.columns.tolist() == ['my_value', 
                                                '_viadot_source', 
                                                '_viadot_downloaded_at_utc']


def test_wrong_query_to_df(BIGQ):
    # Testing error handling for inccorect query for to_df() function.
    fake_query = """
            SELECT fake_name
            FROM `bigquery-public-data.usa_names.fake_table`
            ORDER BY fake_name DESC 
            LIMIT 4
            """
    with pytest.raises(DBDataAccessError):
        BIGQ.to_df(fake_query)