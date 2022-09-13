import json
from typing import Any, Dict, List, Literal

import prefect
from prefect import Task
from prefect.tasks.secrets import PrefectSecret

from viadot.config import local_config
from viadot.sources.sftp import SftpConnector

from .azure_key_vault import AzureKeyVaultSecret


class SftpToDF(Task):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):
        """
        Task for obtaining data from MySql source.

        Args:
            credentials (Dict[str, Any], optional): MySql Database credentials. Defaults to None.

        Returns: Pandas DataFrame
        """
        self.credentials = credentials

        super().__init__(
            name="SftpToDF",
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download file-like object from SFTP server to df"""
        return super().__call__(*args, **kwargs)

    def run(
        self,
        credentials: Dict[str, Any] = None,
        credentials_secret: str = None,
        vault_name: str = None,
        file_name: str = None,
        sep: str = "\t",
        columns: List[str] = None,
    ):
        logger = prefect.context.get("logger")
        if not credentials_secret:
            try:
                credentials_secret = PrefectSecret("QWASI_SFTP").run()
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
            credentials = local_config.get("QWASI_SFTP")
            logger.info("Loaded credentials from local source.")

        if columns is not None and not isinstance(columns, list):
            raise ValueError("Columns paramter must be a list.")

        sftp = SftpConnector(credentials_sftp=credentials)
        sftp.get_conn()
        logger.info("Connected to SFTP server.")
        df = sftp.to_df(file_name=file_name, sep=sep, columns=columns)
        logger.info("Succefully downloaded file from SFTP server.")
        return df
