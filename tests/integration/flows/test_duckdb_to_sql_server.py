import json
import logging
import os
from unittest import mock

import pytest
from prefect.tasks.secrets import PrefectSecret

from viadot.flows import DuckDBToSQLServer
from viadot.sources import DuckDB
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret

TABLE = "test_table"
SCHEMA = "test_schema"
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
            duckdb_schema=SCHEMA,
            duckdb_table=TABLE,
        )
        flow.run()
        mock_method.assert_called_with()
