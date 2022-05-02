import json
import logging
from viadot.tasks import SQLServerCreateTable
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
from prefect.tasks.secrets import PrefectSecret

SCHEMA = "sandbox"
TABLE = "test"


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
        "Date": "DATE",
        "profile": "VARCHAR(255)",
        "Campaignname": "VARCHAR(255)",
        "Impressions": "FLOAT(24)",
        "Clicks": "FLOAT(24)",
        "Cost_eur": "FLOAT(24)",
        "SearchImpressionShare": "VARCHAR(255)",
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
