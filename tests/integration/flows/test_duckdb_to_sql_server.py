import os
import json
import pytest
import logging
from viadot.flows import DuckDBToSQLServer
from unittest import mock
from viadot.sources import DuckDB
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
from prefect.tasks.secrets import PrefectSecret

TABLE = "test_table"
SCHEMA = "test_schema"
TABLE_MULTIPLE_PARQUETS = "test_multiple_parquets"
DATABASE_PATH = "test_db_123.duckdb"


@pytest.fixture(scope="session")
def duckdb():
    duckdb = DuckDB(credentials=dict(database=DATABASE_PATH))
    yield duckdb
    os.remove(DATABASE_PATH)


def test__check_if_schema_exists(duckdb):
    duckdb.run(f"CREATE SCHEMA {SCHEMA}")
    assert not duckdb._check_if_schema_exists(SCHEMA)


def test_create_table_from_parquet(duckdb, TEST_PARQUET_FILE_PATH, caplog):
    with caplog.at_level(logging.INFO):
        duckdb.create_table_from_parquet(
            schema=SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
        )

    assert "created successfully" in caplog.text


def test_duckdb_sql_server_init():

    flow = DuckDBToSQLServer("test_duckdb_init")
    assert flow


def test_duckdb_sql_server_flow():

    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_SQLDB_SERVICE_PRINCIPAL_SECRET"
    ).run()
    vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()
    azure_secret_task = AzureKeyVaultSecret()
    credentials_str = azure_secret_task.run(
        secret=credentials_secret, vault_name=vault_name
    )

    flow = DuckDBToSQLServer(
        "test_duckdb_flow_run",
        duckdb_credentials=dict(database=DATABASE_PATH),
        sql_server_credentials=json.loads(credentials_str),
        sql_server_schema="sandbox",
        sql_server_table=TABLE,
        duckdb_schema=SCHEMA,
        duckdb_table=TABLE,
    )
    r = flow.run()
    assert r.is_successful()


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
