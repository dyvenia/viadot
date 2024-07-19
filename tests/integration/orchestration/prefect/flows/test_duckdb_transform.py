from viadot.orchestration.prefect.flows import duckdb_transform
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


def test_duckdb_transform(duckdb):
    duckdb_transform(f"DROP SCHEMA IF EXISTS {SCHEMA}", duckdb_credentials=DUCKDB_CREDS)
    duckdb_transform(f"CREATE SCHEMA {SCHEMA}", duckdb_credentials=DUCKDB_CREDS)
    duckdb_transform(
        f"""CREATE TABLE {SCHEMA}.{TABLE} (
    i INTEGER NOT NULL,
    decimalnr DOUBLE CHECK (decimalnr < 10),
    date DATE UNIQUE,
    time TIMESTAMP);""",
        duckdb_credentials=DUCKDB_CREDS,
    )
    assert SCHEMA in duckdb.schemas
    duckdb_transform(
        f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", duckdb_credentials=DUCKDB_CREDS
    )
    assert SCHEMA not in duckdb.schemas
