from typing import List
import pandas as pd
import pandas_gbq
import pytest
from numpy import ndarray

from viadot.exceptions import CredentialError, DBDataAccessError
from viadot.sources import BigQuery


@pytest.fixture(scope="function")
def BIGQ():
    BQ = BigQuery(credentials_key="BIGQUERY_TESTS")
    yield BQ


@pytest.fixture(scope="session")
def inset_into_tables():
    table_id1 = "manigeo.manigeo_tab"
    table_id2 = "manigeo.space"
    df = pd.DataFrame({"my_value": ["val1", "val2", "val3"]})
    pandas_gbq.to_gbq(df, table_id1, if_exists="replace")
    pandas_gbq.to_gbq(df, table_id2, if_exists="replace")


QUERY = """
        SELECT name, SUM(number) AS total
        FROM `bigquery-public-data.usa_names.usa_1910_2013`
        GROUP BY name, gender
        ORDER BY total DESC 
        LIMIT 4
        """


def test_credentials():
    with pytest.raises(CredentialError, match=r"Credentials not found."):
        _ = BigQuery(credentials_key="BIGQUERY_TESTS_FAKE")


def test_list_project(BIGQ):
    project = BIGQ.get_project_id()

    assert project == "manifest-geode-341308"


def test_list_datasets(BIGQ):
    datasets = list(BIGQ.list_datasets())

    assert datasets == ["manigeo", "official_empty"]


def test_list_tables(BIGQ):
    datasets = BIGQ.list_datasets()
    tables = list(BIGQ.list_tables(datasets[0]))
    assert "space" and "manigeo_tab" in tables


def test_list_columns(BIGQ):
    columns = BIGQ.list_columns(dataset_name="manigeo", table_name="space")

    assert "my_value" in columns
    assert isinstance(columns, ndarray)


def test_query_is_df(BIGQ):
    df = BIGQ.query_to_df(QUERY)

    assert isinstance(df, pd.DataFrame)


def test_query(BIGQ):
    df = BIGQ.query_to_df(QUERY)
    total_received = df["total"].values

    assert total_received == [4924235, 4818746, 4703680, 4280040]


def test_wrong_query(BIGQ):
    fake_query = """
            SELECT fake_name
            FROM `bigquery-public-data.usa_names.fake_table`
            ORDER BY fake_name DESC 
            LIMIT 4
            """
    with pytest.raises(DBDataAccessError):
        _ = BIGQ.query_to_df(fake_query)
