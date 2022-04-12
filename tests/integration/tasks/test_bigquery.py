import pytest
import logging
import pandas as pd
from viadot.tasks import BigQueryToDF

logger = logging.getLogger(__name__)
DATASET_NAME = "official_empty"
TABLE_NAME = "space"


def test_bigquery_to_df_success():
    bigquery_to_df_task = BigQueryToDF(
        dataset=DATASET_NAME,
        table=TABLE_NAME,
        date_column_name="date",
        credentials_key="BIGQUERY_TESTS",
    )
    df = bigquery_to_df_task.run()
    expectation_columns = ["date", "name", "count", "refresh"]

    assert isinstance(df, pd.DataFrame)
    assert expectation_columns == list(df.columns)


def test_bigquery_to_df_wrong_table_name(caplog):
    bigquery_to_df_task = BigQueryToDF()
    with caplog.at_level(logging.WARNING):
        bigquery_to_df_task.run(
            dataset=DATASET_NAME,
            table="wrong_table_name",
            date_column_name="date",
            credentials_key="BIGQUERY_TESTS",
        )
    assert f"Returning empty data frame." in caplog.text


def test_bigquery_to_df_wrong_column_name(caplog):
    bigquery_to_df_task = BigQueryToDF(
        dataset=DATASET_NAME,
        table=TABLE_NAME,
        date_column_name="wrong_column_name",
        credentials_key="BIGQUERY_TESTS",
    )
    with caplog.at_level(logging.WARNING):
        bigquery_to_df_task.run()
    assert f"'wrong_column_name' column is not recognized." in caplog.text
