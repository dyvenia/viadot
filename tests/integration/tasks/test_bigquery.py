import logging

import pandas as pd
import pytest

from viadot.tasks import BigQueryToDF

logger = logging.getLogger(__name__)
DATASET_NAME = "manigeo"
TABLE_NAME = "space"
CREDENTIALS_KEY = "BIGQUERY-TESTS"


def test_bigquery_to_df_success():
    bigquery_to_df_task = BigQueryToDF(
        query=f"SELECT * FROM `manifest-geode-341308.{DATASET_NAME}.{TABLE_NAME}`",
        credentials_key=CREDENTIALS_KEY,
    )
    df = bigquery_to_df_task.run()
    expected_column = ["my_value", "_viadot_source"]

    assert isinstance(df, pd.DataFrame)
    assert expected_column == list(df.columns)


def test_bigquery_to_df_wrong_table_name(caplog):
    bigquery_to_df_task = BigQueryToDF()
    with caplog.at_level(logging.WARNING):
        df = bigquery_to_df_task.run(
            dataset_name=DATASET_NAME,
            table_name="wrong_table_name",
            date_column_name="date",
            credentials_key=CREDENTIALS_KEY,
        )
    assert f"Returning empty data frame." in caplog.text
    assert df.empty


def test_bigquery_to_df_wrong_column_name(caplog):
    bigquery_to_df_task = BigQueryToDF(
        dataset_name=DATASET_NAME,
        table_name=TABLE_NAME,
        date_column_name="wrong_column_name",
        credentials_key=CREDENTIALS_KEY,
    )
    with caplog.at_level(logging.WARNING):
        df = bigquery_to_df_task.run()
    assert f"'wrong_column_name' column is not recognized." in caplog.text
    assert isinstance(df, pd.DataFrame)


def test_bigquery_to_df_wrong_query(caplog):
    bigquery_to_df_task = BigQueryToDF(
        query="SELECT * FROM table_name",
        credentials_key=CREDENTIALS_KEY,
    )
    with caplog.at_level(logging.WARNING):
        df = bigquery_to_df_task.run()
    assert f"The query is invalid. Please enter a valid query." in caplog.text
    assert df.empty
