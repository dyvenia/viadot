from viadot.orchestration.prefect.tasks import duckdb_query
from viadot.sources import DuckDB 

import pytest
import os

TABLE = "test_table"
SCHEMA = "test_schema"
DATABASE_PATH = "test_db_123.duckdb"
DUCKDB_CREDS = dict(database=DATABASE_PATH, read_only=False)

@pytest.fixture(scope="module")
def duckdb():
    duckdb = DuckDB(credentials=DUCKDB_CREDS)
    yield duckdb
    os.remove(DATABASE_PATH)


def test_duckdb_query(duckdb):
    duckdb_query(f"DROP SCHEMA IF EXISTS {SCHEMA}", credentials = DUCKDB_CREDS)
    duckdb_query(f"CREATE SCHEMA {SCHEMA}", credentials = DUCKDB_CREDS)
    duckdb._check_if_schema_exists(schema = SCHEMA) ==True
    duckdb_query(f"DROP SCHEMA IF EXISTS {SCHEMA}", credentials = DUCKDB_CREDS)
    duckdb._check_if_schema_exists(schema = SCHEMA) ==False

