import pytest
import logging
import pandas as pd
from viadot.tasks import BigQueryToDF
from viadot.exceptions import DBDataAccessError

logger = logging.getLogger(__name__)
PROJECT_NAME = "manifest-geode-341308"
DATESET_NAME = "official_empty"
TABLE_NAME = "space"


def test_bigquery_to_df_success():
    bigquery_to_df_task = BigQueryToDF()
    df = bigquery_to_df_task.run(
        project=PROJECT_NAME,
        dataset=DATESET_NAME,
        table=TABLE_NAME,
        date_column_name="date",
        credentials_key="BIGQUERY_TESTS",
    )
    expectation_columns = ["date", "name", "count", "refresh"]

    assert isinstance(df, pd.DataFrame)
    assert expectation_columns == list(df.columns)


def test_bigquery_to_df_wrong_table_name():
    bigquery_to_df_task = BigQueryToDF()
    with pytest.raises(DBDataAccessError, match=r"Wrong dataset name or table name!"):
        bigquery_to_df_task.run(
            project=PROJECT_NAME,
            dataset=DATESET_NAME,
            table="wrong_table_name",
            date_column_name="date",
            credentials_key="BIGQUERY_TESTS",
        )


def test_bigquery_to_df_wrong_column_name(caplog):
    bigquery_to_df_task = BigQueryToDF()
    with caplog.at_level(logging.INFO):
        bigquery_to_df_task.run(
            project=PROJECT_NAME,
            dataset=DATESET_NAME,
            table=TABLE_NAME,
            date_column_name="wrong_column_name",
            credentials_key="BIGQUERY_TESTS",
        )
    assert (
        f"'wrong_column_name' column not recognized. Dawnloading all the data from '{TABLE_NAME}'"
        in caplog.text
    )
