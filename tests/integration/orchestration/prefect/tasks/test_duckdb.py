from pathlib import Path

import pytest

from viadot.orchestration.prefect.tasks import duckdb_query
from viadot.sources import DuckDB


TABLE = "test_table"
SCHEMA = "test_schema"
DATABASE_PATH = "test_db_123.duckdb"
DUCKDB_CREDS = {"database": DATABASE_PATH, "read_only": False}


@pytest.fixture(scope="module")
def duckdb():
    duckdb = DuckDB(credentials=DUCKDB_CREDS)
    yield duckdb
    Path(DATABASE_PATH).unlink()


def test_duckdb_query(duckdb):
    duckdb_query(f"DROP SCHEMA IF EXISTS {SCHEMA}", credentials=DUCKDB_CREDS)
    duckdb_query(
        f"""CREATE TABLE {SCHEMA}.{TABLE} (
    i INTEGER NOT NULL,
    decimalnr DOUBLE CHECK (decimalnr < 10),
    date DATE UNIQUE,
    time TIMESTAMP);""",
        duckdb_credentials=DUCKDB_CREDS,
    )
    assert SCHEMA in duckdb.schemas
    duckdb_query(
        f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", duckdb_credentials=DUCKDB_CREDS
    )
    assert SCHEMA not in duckdb.schemas
