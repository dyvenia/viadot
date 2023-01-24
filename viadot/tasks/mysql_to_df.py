import json
from typing import Any, Dict, Literal

import prefect
from prefect import Task
from prefect.tasks.secrets import PrefectSecret

from viadot.config import local_config
from viadot.sources.mysql import MySQL

from .azure_key_vault import AzureKeyVaultSecret


class MySqlToDf(Task):
    def __init__(
        self,
        country_short: Literal["AT", "DE", "CH", None],
        credentials: Dict[str, Any] = None,
        query: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        Task for obtaining data from MySql source.

        Args:
            credentials (Dict[str, Any], optional): MySql Database credentials. Defaults to None.
            query(str, optional): Query to perform on a database. Defaults to None.
            country_short (Dict[str, Any], optional): Country short to select proper credential.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.

        Returns: Pandas DataFrame
        """
        self.credentials = credentials
        self.country_short = country_short
        self.query = query

        super().__init__(
            name="MySQLToDF",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download from MySQL database to df"""
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
                credentials_secret = PrefectSecret("CONVIDERA").run()
                logger.info("Loaded credentials from Key Vault.")
            except ValueError:
                pass

        if credentials_secret:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials = json.loads(credentials_str)
            logger.info("Loaded credentials from Key Vault.")
        else:
            credentials = local_config.get("CONVIDERA")
            logger.info("Loaded credentials from local source.")

        country_cred = credentials.get(f"{self.country_short}")
        ssh_creds = credentials.get("SSH_CREDS")
        credentials_country = dict(country_cred, **ssh_creds)
        mysql = MySQL(credentials=credentials_country)
        logger.info("Connected to MySql Database.")
        df = mysql.connect_sql_ssh(query=query)
        logger.info("Succefully collected data from query")
        return df
