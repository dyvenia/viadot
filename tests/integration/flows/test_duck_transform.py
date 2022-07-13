import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import DuckDBTransform
from viadot.sources import DuckDB

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
