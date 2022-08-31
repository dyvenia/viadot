import json
from unittest import mock

from viadot.flows import DuckDBToSQLServer

TABLE = "test_table"
DUCKDB_SCHEMA = "test_schema"
SQL_SERVER_SCHEMA = "sandbox"
TABLE_MULTIPLE_PARQUETS = "test_multiple_parquets"
DATABASE_PATH = "test_db_123.duckdb"


def test_duckdb_sql_server_init():

    flow = DuckDBToSQLServer("test_duckdb_init")
    assert flow


def test_duckdb_sql_server_flow_mocked():
    with mock.patch.object(DuckDBToSQLServer, "run", return_value=True) as mock_method:
        flow = DuckDBToSQLServer(
            "test_duckdb_flow_run",
            sql_server_table=TABLE,
            duckdb_schema=DUCKDB_SCHEMA,
            duckdb_table=TABLE,
        )
        flow.run()
        mock_method.assert_called_with()
