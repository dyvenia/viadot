from typing import List

import pandas as pd
import pandas_gbq
import pytest
from numpy import ndarray

from viadot.exceptions import CredentialError, DBDataAccessError
from viadot.sources import BigQuery


@pytest.fixture(scope="function")
def BIGQ():
    """
    Fixture for creating a BigQuery class instance. This fixture initializes a BigQuery client
    using the provided credentials key and yields the class instance.
    The class instance can be used within a test function to interact with BigQuery.

    Yields:
        BigQuery: A BigQuery client instance.
    """
    BQ = BigQuery(credentials_key="BIGQUERY_TESTS")
    yield BQ


@pytest.fixture(scope="session")
def insert_into_tables() -> None:
    """
    A function to insert data into a BigQuery table. In the current version, tables are deleted
    after 60 days. This operation is used to secure tests and structure in a BigQuery project.
    """
    table_id1 = "manigeo.manigeo_tab"
    table_id2 = "manigeo.space"
    df = pd.DataFrame({"my_value": ["val1", "val2", "val3"]})
    pandas_gbq.to_gbq(df, table_id1, if_exists="replace")
    pandas_gbq.to_gbq(df, table_id2, if_exists="replace")


# SQL query for public dataset - user with access to Bigquery can also use public datasets and tables.
QUERY = """
        SELECT name, SUM(number) AS total
        FROM `bigquery-public-data.usa_names.usa_1910_2013`
        GROUP BY name, gender
        ORDER BY total DESC 
        LIMIT 4
        """


def test_credentials():
    """Test to see if an exception is thrown if credentials are not provided."""
    with pytest.raises(CredentialError, match=r"Credentials not found."):
        BigQuery(credentials_key="BIGQUERY_TESTS_FAKE")


def test_list_project(BIGQ):
    """
    Testing the correctness of the project name.

    Args:
        BIGQ (Bigquery): Bigquery class instance.
    """
    project = BIGQ.get_project_id()

    assert project == "manifest-geode-341308"


def test_list_datasets(BIGQ):
    """
    Testing the correctness of dataset names.

    Args:
        BIGQ (Bigquery): Bigquery class instance.
    """
    datasets = list(BIGQ.list_datasets())

    assert datasets == ["manigeo", "official_empty"]


def test_list_tables(BIGQ):
    """
    Testing the correctness of table names.

    Args:
        BIGQ (Bigquery): Bigquery class instance.
    """
    datasets = BIGQ.list_datasets()
    tables = list(BIGQ.list_tables(datasets[0]))
    assert "space" and "manigeo_tab" in tables


def test_list_columns(BIGQ):
    """
    Testing the validity of a column name in a specific table in BigQuery and the return type.

    Args:
        BIGQ (Bigquery): Bigquery class instance.
    """
    columns = BIGQ.list_columns(dataset_name="manigeo", table_name="space")

    assert "my_value" in columns
    assert isinstance(columns, ndarray)


def test_query_is_df(BIGQ):
    """
    Testing the return type of `query_to_df` function. It should be a Data Frame.

    Args:
        BIGQ (Bigquery): Bigquery class instance.
    """
    df = BIGQ.query_to_df(QUERY)

    assert isinstance(df, pd.DataFrame)


def test_query(BIGQ):
    """
    Testing the corectness of `query_to_df`execution.

    Args:
        BIGQ (Bigquery): Bigquery class instance.
    """
    df = BIGQ.query_to_df(QUERY)
    total_received = df["total"].values

    assert total_received == [4924235, 4818746, 4703680, 4280040]


def test_wrong_query(BIGQ):
    """
    Testing if the exception is raised with invalid query.

    Args:
        BIGQ (Bigquery): Bigquery class instance.
    """
    fake_query = """
            SELECT fake_name
            FROM `bigquery-public-data.usa_names.fake_table`
            ORDER BY fake_name DESC 
            LIMIT 4
            """
    with pytest.raises(DBDataAccessError):
        BIGQ.query_to_df(fake_query)
