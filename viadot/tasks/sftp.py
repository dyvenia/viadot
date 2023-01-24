import json
from typing import Any, Dict, List

import prefect
from prefect import Task
from prefect.tasks.secrets import PrefectSecret

from viadot.config import local_config
from viadot.sources.sftp import SftpConnector

from .azure_key_vault import AzureKeyVaultSecret
import time


class SftpToDF(Task):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        sftp_credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """Task for downloading data from SFTP server to pandas DataFrame.

        Args:
            credentials (Dict[str, Any], optional): SFTP credentials. Defaults to None.
            sftp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary credentials for SFTP connection. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.

        Returns: Pandas DataFrame
        """
        self.credentials = credentials
        self.sftp_credentials_secret = sftp_credentials_secret
        self.vault_name = vault_name

        super().__init__(
            name="SftpToDF",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download file-like object from SFTP server to df"""
        return super().__call__(*args, **kwargs)

    def run(
        self,
        from_path: str = None,
        sep: str = "\t",
        columns: List[str] = None,
        **kwargs,
    ):
        logger = prefect.context.get("logger")
        if not self.credentials:
            if not self.sftp_credentials_secret:
                try:
                    self.sftp_credentials_secret = PrefectSecret("QWASI-SFTP").run()
                    logger.info("Loaded credentials from Key Vault.")
                except ValueError:
                    pass

            if self.sftp_credentials_secret:
                credentials_str = AzureKeyVaultSecret(
                    self.sftp_credentials_secret, vault_name=self.vault_name
                ).run()
                credentials_sftp = json.loads(credentials_str)
                logger.info("Loaded credentials from Key Vault.")
            else:
                credentials_sftp = local_config.get("QWASI-SFTP")
                logger.info("Loaded credentials from local source.")
        else:
            credentials_sftp = self.credentials

        sftp = SftpConnector(credentials_sftp=credentials_sftp)
        sftp.get_conn()
        logger.info("Connected to SFTP server.")
        time.sleep(1)
        df = sftp.to_df(file_name=from_path, sep=sep, columns=columns, **kwargs)
        logger.info("Succefully downloaded file from SFTP server.")
        return df


class SftpList(Task):
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        sftp_credentials_secret: str = None,
        vault_name: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """Task for listing files in SFTP server.

        Args:
            credentials (Dict[str, Any], optional): SFTP credentials. Defaults to None.
            sftp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary credentials for SFTP connection. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.

        Returns:
            files_list (List): List of files in SFTP server.
        """
        self.credentials = credentials
        self.sftp_credentials_secret = sftp_credentials_secret
        self.vault_name = vault_name

        super().__init__(
            name="SftpList",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Task for listing files in SFTP server."""
        return super().__call__(*args, **kwargs)

    def run(
        self,
        path: str = None,
        recursive: bool = False,
        matching_path: str = None,
    ):
        logger = prefect.context.get("logger")

        if not self.credentials:
            if not self.sftp_credentials_secret:
                try:
                    self.sftp_credentials_secret = PrefectSecret("QWASI-SFTP").run()
                    logger.info("Loaded credentials from Key Vault.")
                except ValueError:
                    pass

            if self.sftp_credentials_secret:
                credentials_str = AzureKeyVaultSecret(
                    self.sftp_credentials_secret, vault_name=self.vault_name
                ).run()
                credentials_sftp = json.loads(credentials_str)
                logger.info("Loaded credentials from Key Vault.")
            else:
                credentials_sftp = local_config.get("QWASI-SFTP")
                logger.info("Loaded credentials from local source.")
        else:
            credentials_sftp = self.credentials

        sftp = SftpConnector(credentials_sftp=credentials_sftp)
        sftp.get_conn()
        logger.info("Connected to SFTP server.")
        if recursive is False:
            files_list = sftp.list_directory(path=path)

        else:
            files_list = sftp.recursive_listdir(path=path)
            files_list = sftp.process_defaultdict(defaultdict=files_list)
        if matching_path is not None:
            files_list = [f for f in files_list if matching_path in f]

        logger.info("Succefully loaded file list from SFTP server.")

        return files_list
