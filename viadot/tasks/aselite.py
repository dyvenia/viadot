import json
from typing import Any, Dict

import prefect
from prefect import Task
from prefect.tasks.secrets import PrefectSecret

from viadot.config import local_config
from viadot.sources import AzureSQL

from .azure_key_vault import AzureKeyVaultSecret


class ASELiteToDF(Task):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        query: str = None,
        timeout: int = 3600,
        *args,
        **kwargs
    ):
        """
        Task for obtaining data from ASElite source.
        Args:
            credentials (Dict[str, Any], optional): ASElite SQL Database credentials. Defaults to None.
            query(str, optional): Query to perform on a database. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        Returns: Pandas DataFrame
        """
        self.credentials = credentials
        self.query = query

        super().__init__(
            name="ASElite_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download from aselite database to df"""
        return super().__call__(*args, **kwargs)

    def run(
        self,
        query: str,
        credentials: Dict[str, Any] = None,
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

        aselite = AzureSQL(credentials=credentials)
        logger.info("Connected to ASELITE SOURCE")
        df = aselite.to_df(query=query)
        logger.info("Succefully collected data from query")
        return df
