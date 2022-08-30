import pandas as pd

from viadot.sources import BigQuery

BIGQ = BigQuery(credentials_key="BIGQUERY_TESTS")


def test_list_project():
    project = BIGQ.get_project_id()
    assert project == "manifest-geode-341308"


def test_list_datasets():
    datasets = list(BIGQ.list_datasets())
    assert datasets == ["manigeo", "official_empty"]


def test_list_tables():
    datasets = BIGQ.list_datasets()
    tables = list(BIGQ.list_tables(datasets[0]))
    assert tables == ["space", "test_data", "manigeo_tab"]


def test_query_is_df():
    query = """
            SELECT name, SUM(number) AS total
            FROM `bigquery-public-data.usa_names.usa_1910_2013`
            GROUP BY name, gender
            ORDER BY total DESC 
            LIMIT 4
            """
    df = BIGQ.query_to_df(query)

    assert isinstance(df, pd.DataFrame)


def test_query():
    query = """
            SELECT name, SUM(number) AS total
            FROM `bigquery-public-data.usa_names.usa_1910_2013`
            GROUP BY name, gender
            ORDER BY total DESC 
            LIMIT 4
            """
    df = BIGQ.query_to_df(query)
    total_received = df["total"].values

    assert total_received == [4924235, 4818746, 4703680, 4280040]
