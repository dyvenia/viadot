import json
import pytest
import os
import logging
from prefect.tasks.secrets import PrefectSecret

from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
from viadot.flows import DuckDBToSQLServer
from viadot.sources import DuckDB

TABLE = "test_table"
DUCKDB_SCHEMA = "test_schema"
SQL_SERVER_SCHEMA = "sandbox"
TABLE_MULTIPLE_PARQUETS = "test_multiple_parquets"
DATABASE_PATH = "test_db_123.duckdb"


@pytest.fixture(scope="session")
def duckdb():
    duckdb = DuckDB(credentials=dict(database=DATABASE_PATH))
    yield duckdb
    os.remove(DATABASE_PATH)


def test_create_table_from_parquet(duckdb, TEST_PARQUET_FILE_PATH, caplog):
    with caplog.at_level(logging.INFO):
        duckdb.create_table_from_parquet(
            schema=DUCKDB_SCHEMA, table=TABLE, path=TEST_PARQUET_FILE_PATH
        )

    assert "created successfully" in caplog.text


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
        duckdb_schema=DUCKDB_SCHEMA,
        duckdb_table=TABLE,
    )
    r = flow.run()
    assert r.is_successful()
