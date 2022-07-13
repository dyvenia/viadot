import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import DuckDBTransform
from viadot.sources import DuckDB
from viadot.tasks import DuckDBQuery, DuckDBToDF

TABLE = "test_table"
SCHEMA = "test_schema"
TABLE_MULTIPLE_PARQUETS = "test_multiple_parquets"
DATABASE_PATH = "test_db_123.duckdb"


@pytest.fixture(scope="session")
def duckdb():
    duckdb = DuckDB(credentials=dict(database=DATABASE_PATH))
    yield duckdb
    os.remove(DATABASE_PATH)


def test_create_table_from_parquet(duckdb, TEST_PARQUET_FILE_PATH):
    duckdb.create_table_from_parquet(
        schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
    )


def test_duckdb_query():

    db_query = DuckDBQuery(credentials=dict(database=DATABASE_PATH))

    result = db_query.run(f"select * from {SCHEMA}.{TABLE}")
    assert type(result) == list
    assert len(result) > 1


def test_duckdb_to_df():

    instance = DuckDBToDF(
        schema=SCHEMA, table=TABLE, credentials=dict(database=DATABASE_PATH)
    )
    test_df = instance.run()
    assert test_df.shape > (1, 1)
    assert type(test_df) == pd.core.frame.DataFrame


def test_duckdb_transform_init():
    instance = DuckDBTransform("test_duckdb_transform", query="select * from test")

    assert instance


def test_duckdb_transform_flow_run():
    instance = DuckDBTransform(
        "test_duckdb_transform",
        query=f"select * from {SCHEMA}.{TABLE}",
        credentials=dict(database=DATABASE_PATH),
    )
    result = instance.run()
    assert result.is_successful()
