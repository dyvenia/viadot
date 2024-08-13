from pathlib import Path

from viadot.orchestration.prefect.flows import duckdb_to_parquet
from viadot.sources import DuckDB


DUCKDB_CREDS = {"database": "test.duckdb", "read_only": False}
PATH = "test_parquet.parquet"


def test_duckdb_to_parquet():
    assert not Path(PATH).exists()

    duckdb = DuckDB(credentials=DUCKDB_CREDS)
    duckdb.run_query(
        query="""
--CREATE SCHEMA sandbox;
CREATE or replace TABLE sandbox.numbers AS
SELECT 42 AS i, 84 AS j;
"""
    )
    duckdb_to_parquet(
        query="""SELECT * FROM sandbox.numbers""",
        path=PATH,
        duckdb_credentials=DUCKDB_CREDS,
        if_exists="replace",
    )

    assert Path(PATH).exists()
    Path(PATH).unlink()
