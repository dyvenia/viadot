import os

import pytest

from viadot.sources.duckdb import DuckDB
from viadot.tasks import DuckDBCreateTableFromParquet

TABLE = "test_table"
SCHEMA = "test_schema"
DATABASE_PATH = "test_db_123.duckdb"


@pytest.fixture(scope="module")
def duckdb():
    duckdb = DuckDB(credentials=dict(database=DATABASE_PATH))
    yield duckdb


def test_create_table_empty_file(duckdb):
    path = "empty.parquet"
    with open(path, "w"):
        pass
    duckdb_creds = {f"database": DATABASE_PATH}
    task = DuckDBCreateTableFromParquet(credentials=duckdb_creds)
    task.run(schema=SCHEMA, table=TABLE, path=path, if_empty="skip")

    assert duckdb._check_if_table_exists(TABLE, schema=SCHEMA) == False
    os.remove(path)


def test_create_table(duckdb, TEST_PARQUET_FILE_PATH):
    duckdb_creds = {f"database": DATABASE_PATH}
    task = DuckDBCreateTableFromParquet(credentials=duckdb_creds)
    task.run(schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH, if_empty="skip")

    assert duckdb._check_if_table_exists(TABLE, schema=SCHEMA)
    os.remove(DATABASE_PATH)
