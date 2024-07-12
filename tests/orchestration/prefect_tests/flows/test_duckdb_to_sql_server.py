from viadot.orchestration.prefect.flows import duckdb_to_sql_server
from viadot.orchestration.prefect.tasks import sql_server_query
from viadot.sources import SQLServer, DuckDB

import os
import pytest

TABLE = "test_table"
SCHEMA = "sandbox"
DATABASE_PATH = "test_db_123.duckdb"
DUCKDB_CREDS = dict(database=DATABASE_PATH, read_only=False)


@pytest.fixture
def sql_server():
    # Initialize the SQLServer instance with the test credentials
    sql_server = SQLServer(config_key = "sql_server")
    yield sql_server


@pytest.fixture
def duckdb():
    # Initialize the SQLServer instance with the test credentials
    duckdb = DuckDB(credentials=DUCKDB_CREDS)
    duckdb.run_query(
        query="""
        CREATE SCHEMA sandbox;
        CREATE or replace TABLE sandbox.numbers AS
        SELECT 42 AS i, 84 AS j;
                        """
    )
    yield duckdb
    os.remove(DATABASE_PATH)
    
def test_duckdb_to_sql_server(duckdb, sql_server):
    duckdb_to_sql_server(
    query="select * from sandbox.numbers",
    local_path = "testing.csv",
    db_table= TABLE,
    db_schema = SCHEMA,
    duckdb_credentials = DUCKDB_CREDS,
    sql_server_credentials_secret = "sql-server"
    )
    assert sql_server.exists(table = TABLE, schema= SCHEMA) == True

    sql_server_query(query =f"""DROP TABLE {SCHEMA}.{TABLE}""",credentials_secret = "sql-server")
