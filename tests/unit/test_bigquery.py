import pytest
from viadot.sources import BigQuery


BIGQ = BigQuery(credentials_key="BIGQUERY_TESTS")


def test_list_project():
    proj = BIGQ.list_projects()
    assert proj == "wise-trainer-342008"


def test_list_datasets():
    datasets = BIGQ.list_datasets()
    assert ["test_data", "wise"] == datasets


def test_list_tables():
    datasets = BIGQ.list_datasets()
    tables = BIGQ.list_tables(datasets[0])
    assert ["crm_data"] == tables


def test_query():
    query = """
            SELECT name, SUM(number) AS total
            FROM `bigquery-public-data.usa_names.usa_1910_2013`
            GROUP BY name, gender
            ORDER BY total DESC 
            LIMIT 4
            """
    names = {}
    query_job = BIGQ.query(query)
    for row in query_job:
        names[row["name"]] = row["total"]

    assert names == {
        "James": 4924235,
        "John": 4818746,
        "Robert": 4703680,
        "Michael": 4280040,
    }
