from typing import Any, Dict, List

from prefect import Flow

from ..tasks.azure_sql import AzureSQLDBQuery

query_task = AzureSQLDBQuery()


class AzureSQLTransform(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        sqldb_credentials_secret: str = None,
        vault_name: str = None,
        tags: List[str] = ["transform"],
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for running SQL queries on top of Azure SQL Database.

        Args:
            name (str): The name of the flow.
            query (str, required): The query to execute on the database.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with SQL db credentials (server, db_name, user, and password).
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            tags (list, optional): Tag for marking flow. Defaults to "transform".
        """
        self.query = query
        self.tags = tags
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.vault_name = vault_name
        self.tasks = [query_task]

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_task.bind(
            query=self.query,
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
