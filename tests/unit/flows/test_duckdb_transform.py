import os

import pytest

from viadot.flows import DuckDBTransform
from viadot.sources import DuckDB

BRONZE_SCHEMA = "bronze_schema"
SILVER_SCHEMA = "silver_schema"
TABLE = "test_table"
DATABASE_PATH = "test_db_1234.duckdb"
CREDENTIALS = dict(database=DATABASE_PATH)


@pytest.fixture(scope="session")
def duckdb():
    duckdb = DuckDB(credentials=CREDENTIALS)
    duckdb.run(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
    duckdb.run(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

    # create placeholder tables so that we can list schemas later on
    # (DuckDB does not expose a way to list schemas without a table)
    duckdb.run(f"CREATE TABLE {BRONZE_SCHEMA}.placeholder(a INTEGER)")
    duckdb.run(f"CREATE TABLE {SILVER_SCHEMA}.placeholder(a INTEGER)")
    yield duckdb
    os.remove(DATABASE_PATH)


def test_duckdb_transform(duckdb, TEST_PARQUET_FILE_PATH):
    silver_table_fqn = SILVER_SCHEMA + "." + TABLE
    assert silver_table_fqn not in duckdb.tables

    # create a table to transform
    duckdb.create_table_from_parquet(
        schema=BRONZE_SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
    )

    # run the flow
    flow = DuckDBTransform(
        name="First DuckDBTransform flow",
        query=f"CREATE TABLE {SILVER_SCHEMA}.{TABLE} AS SELECT * FROM {BRONZE_SCHEMA}.{TABLE}",
        credentials=CREDENTIALS,
    )
    result = flow.run()

    assert result.is_successful()

    df = duckdb.to_df(f"SELECT * FROM {SILVER_SCHEMA}.{TABLE}")
    assert df.shape[0] == 3
