from prefect import Task
from viadot.sources.base import SQL
from typing import Any, Dict
from prefect.tasks.secrets import PrefectSecret
from .azure_key_vault import AzureKeyVaultSecret
from viadot.config import local_config
import json
import prefect


class ASELiteToDF(Task):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        db_name: str = None,
        query: str = None,
        *args,
        **kwargs
    ):
        """
        Task for obtaining data from ASElite source.
        Args:
            credentials (Dict[str, Any], optional): ASElite SQL Database credentials. Defaults to None.
            db_name(str, optional): Name of ASElite database. Defaults to None.
            query(str, optional): Query to perform on a database. Defaults to None.
        Returns: Pandas DataFrame
        """
        self.credentials = credentials
        self.db_name = db_name
        self.query = query

        super().__init__(
            name="ASElite_to_df",
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download from aselite database to df"""
        return super().__call__(*args, **kwargs)

    def run(
        self,
        credentials: Dict[str, Any] = None,
        # db_name: str = None,
        query: str = None,
        if_empty: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
    ):
        logger = prefect.context.get("logger")
        if not credentials_secret:
            try:
                credentials_secret = PrefectSecret("aselite").run()
            except ValueError:
                pass

        if credentials_secret:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials = json.loads(credentials_str)
            logger.info("Loaded credentials from Key Vault")
        else:
            credentials = local_config.get("ASELite_SQL")
            logger.info("Loaded credentials from local source")

        aselite = SQL(credentials=credentials)  # , db_name=db_name
        logger.info("Connected to ASELITE SOURCE")
        df = aselite.to_df(query=query, if_empty=if_empty)
        logger.info("Succefully collected data from query")
        return df
