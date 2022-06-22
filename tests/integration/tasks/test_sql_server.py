import json
import logging
import inspect
from viadot.tasks import SQLServerCreateTable, SQLServerToDF, SQLServerQuery
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
from prefect.tasks.secrets import PrefectSecret

SCHEMA = "sandbox"
TABLE = "test"


def test_sql_server_create_table_init():
    instance = SQLServerCreateTable()
    name = instance.__dict__["name"]
    assert inspect.isclass(SQLServerCreateTable)
    assert isinstance(instance, SQLServerCreateTable)
    assert name == "sql_server_create_table"


def test_sql_server_create_table(caplog):
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
    with caplog.at_level(logging.INFO):
        create_table_task.run(
            schema=SCHEMA,
            table=TABLE,
            dtypes=dtypes,
            if_exists="replace",
            credentials=json.loads(credentials_str),
        )
        assert "Successfully created table" in caplog.text


def test_sql_server_to_df():
    task = SQLServerToDF(config_key="AZURE_SQL")
    df = task.run(query=f"SELECT * FROM {SCHEMA}.{TABLE}")
    assert df.columns.to_list() == [
        "date",
        "name",
        "id",
        "weather",
        "rain",
        "temp",
        "summary",
    ]


def test_sql_server_query(caplog):
    task = SQLServerQuery(config_key="AZURE_SQL")
    task.run(f"DROP TABLE IF EXISTS sandbox.test_query")
    with caplog.at_level(logging.INFO):
        task.run(f"CREATE TABLE sandbox.test_query (Id INT, Name VARCHAR(10))")
        assert "Successfully ran the query." in caplog.text
    task.run(f"DROP TABLE sandbox.test_query")
