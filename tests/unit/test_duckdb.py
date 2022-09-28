import os

import pytest
import pandas as pd
from viadot.sources.duckdb import DuckDB

TABLE = "test_table"
SCHEMA = "test_schema"
TABLE_MULTIPLE_PARQUETS = "test_multiple_parquets"
DATABASE_PATH = "test_db_123.duckdb"


@pytest.fixture(scope="module")
def duckdb():
    duckdb = DuckDB(credentials=dict(database=DATABASE_PATH))
    yield duckdb
    os.remove(DATABASE_PATH)


def test__check_if_schema_exists(duckdb):

    duckdb.run(f"DROP SCHEMA IF EXISTS {SCHEMA}")
    assert not duckdb._check_if_schema_exists(SCHEMA)

    duckdb.run(f"CREATE SCHEMA {SCHEMA}")
    assert not duckdb._check_if_schema_exists(SCHEMA)

    duckdb.run(f"DROP SCHEMA {SCHEMA}")


def test_create_table_from_parquet(duckdb, TEST_PARQUET_FILE_PATH):
    duckdb.create_table_from_parquet(
        schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
    )
    df = duckdb.to_df(f"SELECT * FROM {SCHEMA}.{TABLE}")
    assert df.shape[0] == 3
    duckdb.drop_table(TABLE, schema=SCHEMA)
    duckdb.run(f"DROP SCHEMA {SCHEMA}")


def test_create_table_from_parquet_append(duckdb, TEST_PARQUET_FILE_PATH):
    duckdb.create_table_from_parquet(
        schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
    )
    df = duckdb.to_df(f"SELECT * FROM {SCHEMA}.{TABLE}")
    assert df.shape[0] == 3

    # now append
    duckdb.create_table_from_parquet(
        schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH, if_exists="append"
    )
    df = duckdb.to_df(f"SELECT * FROM {SCHEMA}.{TABLE}")
    assert df.shape[0] == 6

    duckdb.drop_table(TABLE, schema=SCHEMA)
    duckdb.run(f"DROP SCHEMA {SCHEMA}")


def test_create_table_from_multiple_parquet(duckdb):
    # we use the two Parquet files generated by fixtures in conftest
    duckdb.create_table_from_parquet(
        schema=SCHEMA, table=TABLE_MULTIPLE_PARQUETS, path="*.parquet"
    )
    df = duckdb.to_df(f"SELECT * FROM {SCHEMA}.{TABLE_MULTIPLE_PARQUETS}")
    assert df.shape[0] == 6
    duckdb.drop_table(TABLE_MULTIPLE_PARQUETS, schema=SCHEMA)
    duckdb.run(f"DROP SCHEMA {SCHEMA}")


def test__check_if_table_exists(duckdb, TEST_PARQUET_FILE_PATH):

    assert not duckdb._check_if_table_exists(table=TABLE, schema=SCHEMA)
    duckdb.create_table_from_parquet(
        schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
    )
    assert duckdb._check_if_table_exists(TABLE, schema=SCHEMA)
    duckdb.drop_table(TABLE, schema=SCHEMA)


def test_run_query_with_comments(duckdb, TEST_PARQUET_FILE_PATH):
    duckdb.create_table_from_parquet(
        schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
    )
    output = duckdb.run(
        query=f""" 
        --test 
    SELECT * FROM {SCHEMA}.{TABLE}
    """,
        fetch_type="dataframe",
    )
    assert isinstance(output, pd.DataFrame)
    duckdb.drop_table(TABLE, schema=SCHEMA)
