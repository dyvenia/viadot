from viadot.orchestration.prefect.flows import duckdb_to_parquet
from viadot.sources import DuckDB

import os

DUCKDB_CREDS = {"database": "test.duckdb", "read_only": False}
PATH = "test_parquet.parquet"
def test_duckdb_to_parquet():
    assert os.path.isfile(PATH) == False

    duckdb = DuckDB(credentials=DUCKDB_CREDS)
    duckdb.run_query(
        query="""
        --CREATE SCHEMA sandbox;
        CREATE or replace TABLE sandbox.numbers AS
        SELECT 42 AS i, 84 AS j;
                        """
    )

    flow = duckdb_to_parquet(
        query="""SELECT * FROM sandbox.numbers""",
        path=PATH,
        duckdb_credentials=DUCKDB_CREDS,
        if_exists="replace",
    )

    assert os.path.isfile(PATH) == True
    os.remove(PATH)



# assert file_exists is True

# minio.rm(path=PATH)
