import json

import pytest
from prefect.tasks.secrets import PrefectSecret

from viadot.flows.sql_server_to_duckdb import SQLServerToDuckDB
from viadot.tasks import AzureSQLDBQuery, DuckDBQuery, DuckDBToDF, SQLServerCreateTable
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret

SCHEMA = "sandbox"
TABLE = "test"


@pytest.fixture(scope="session")
def create_sql_server_table():
    credentials_secret = PrefectSecret(
        "AZURE_DEFAULT_SQLDB_SERVICE_PRINCIPAL_SECRET"
    ).run()
    vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()
    azure_secret_task = AzureKeyVaultSecret()
    credentials_str = azure_secret_task.run(
        secret=credentials_secret, vault_name=vault_name
    )
    dtypes = {
        "date": "DATE",
        "name": "VARCHAR(255)",
        "id": "VARCHAR(255)",
        "weather": "FLOAT(24)",
        "rain": "FLOAT(24)",
        "temp": "FLOAT(24)",
        "summary": "VARCHAR(255)",
    }
    create_table_task = SQLServerCreateTable()
    yield create_table_task.run(
        schema=SCHEMA,
        table=TABLE,
        dtypes=dtypes,
        if_exists="replace",
        credentials=json.loads(credentials_str),
    )
    drop_sqlserver = AzureSQLDBQuery()
    drop_sqlserver.run(
        query=f"DROP TABLE {SCHEMA}.{TABLE}", credentials_secret=credentials_secret
    )


def test_sql_server_to_duckdb(create_sql_server_table):
    create_sql_server_table
    duckdb_creds = {"database": "/home/viadot/database/test.duckdb"}
    flow = SQLServerToDuckDB(
        name="test",
        sql_query=f"SELECT * FROM {SCHEMA}.{TABLE}",
        local_file_path="test.parquet",
        sqlserver_config_key="AZURE_SQL",
        if_exists="replace",
        duckdb_table=TABLE,
        duckdb_schema=SCHEMA,
        duckdb_credentials=duckdb_creds,
    )
    result = flow.run()
    assert result.is_successful()

    df_task = DuckDBToDF(credentials=duckdb_creds)
    df = df_task.run(table=TABLE, schema=SCHEMA)

    assert df.columns.to_list() == [
        "date",
        "name",
        "id",
        "weather",
        "rain",
        "temp",
        "summary",
        "_viadot_downloaded_at_utc",
    ]
    drop_duckdb = DuckDBQuery()
    drop_duckdb.run(query=f"DROP TABLE {SCHEMA}.{TABLE}", credentials=duckdb_creds)
