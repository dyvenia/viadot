import json
import os
from datetime import timedelta
from typing import List

import pandas as pd
import numpy as np
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import AzureDataLake
from .azure_key_vault import AzureKeyVaultSecret


class AzureDataLakeDownload(Task):
    """
    Task for downloading data from the Azure Data lakes (gen1 and gen2).

    Args:
        from_path (str, optional): The path from which to download the file(s). Defaults to None.
        to_path (str, optional): The destination path. Defaults to None.
        recursive (bool, optional): Set this to true if downloading entire directories.
        gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
        vault_name (str, optional): The name of the vault from which to fetch the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
        max_retries (int, optional): [description]. Defaults to 3.
        retry_delay (timedelta, optional): [description]. Defaults to timedelta(seconds=10).
    """

    def __init__(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = False,
        gen: int = 2,
        vault_name: str = None,
        timeout: int = 3600,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.from_path = from_path
        self.to_path = to_path
        self.recursive = recursive
        self.gen = gen
        self.vault_name = vault_name

        super().__init__(
            name="adls_download",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download file(s) from the Azure Data Lake"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "from_path",
        "to_path",
        "recursive",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> None:
        """Task run method.

        Args:
            from_path (str): The path from which to download the file(s).
            to_path (str): The destination path.
            recursive (bool): Set this to true if downloading entire directories.
            gen (int): The generation of the Azure Data Lake.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """
        file_name = from_path.split("/")[-1]
        to_path = to_path or file_name

        if not sp_credentials_secret:
            # attempt to read a default for the service principal secret name
            try:
                sp_credentials_secret = PrefectSecret(
                    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
                ).run()
            except ValueError:
                pass

        if sp_credentials_secret:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials)

        full_dl_path = os.path.join(credentials["ACCOUNT_NAME"], from_path)
        self.logger.info(f"Downloading data from {full_dl_path} to {to_path}...")
        lake.download(from_path=from_path, to_path=to_path, recursive=recursive)
        self.logger.info(f"Successfully downloaded data to {to_path}.")


class AzureDataLakeUpload(Task):
    """Upload file(s) to Azure Data Lake.

    Args:
        from_path (str, optional): The local path from which to upload the file(s). Defaults to None.
        to_path (str, optional): The destination path. Defaults to None.
        recursive (bool, optional): Set this to true if uploading entire directories. Defaults to False.
        overwrite (bool, optional): Whether to overwrite files in the lake. Defaults to False.
        gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
    """

    def __init__(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = False,
        overwrite: bool = False,
        gen: int = 2,
        vault_name: str = None,
        timeout: int = 3600,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.from_path = from_path
        self.to_path = to_path
        self.recursive = recursive
        self.overwrite = overwrite
        self.gen = gen
        self.vault_name = vault_name

        super().__init__(
            name="adls_upload",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Upload file(s) to the Azure Data Lake"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "from_path",
        "to_path",
        "recursive",
        "overwrite",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = None,
        overwrite: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> None:
        """Task run method.

        Args:
            from_path (str): The path from which to upload the file(s).
            to_path (str): The destination path.
            recursive (bool): Set to true if uploading entire directories.
            overwrite (bool): Whether to overwrite the file(s) if they exist.
            gen (int): The generation of the Azure Data Lake.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        if not sp_credentials_secret:
            # attempt to read a default for the service principal secret name
            try:
                sp_credentials_secret = PrefectSecret(
                    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
                ).run()
            except ValueError:
                pass

        if sp_credentials_secret:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials)

        full_to_path = os.path.join(credentials["ACCOUNT_NAME"], to_path)
        self.logger.info(f"Uploading data from {from_path} to {full_to_path}...")
        lake.upload(
            from_path=from_path,
            to_path=to_path,
            recursive=recursive,
            overwrite=overwrite,
        )
        self.logger.info(f"Successfully uploaded data to {full_to_path}.")


class AzureDataLakeToDF(Task):
    def __init__(
        self,
        path: str = None,
        sep: str = "\t",
        quoting: int = 0,
        lineterminator: str = None,
        error_bad_lines: bool = None,
        gen: int = 2,
        vault_name: str = None,
        timeout: int = 3600,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        """Load file(s) from the Azure Data Lake to a pandas DataFrame.
        Currently supports CSV and parquet files.

        Args:
            path (str, optional): The path from which to load the DataFrame. Defaults to None.
            sep (str, optional): The separator to use when reading a CSV file. Defaults to "\t".
            quoting (int, optional): The quoting mode to use when reading a CSV file. Defaults to 0.
            lineterminator (str, optional): The newline separator to use when reading a CSV file. Defaults to None.
            error_bad_lines (bool, optional): Whether to raise an exception on bad lines. Defaults to None.
            gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        self.path = path
        self.sep = sep
        self.quoting = quoting
        self.lineterminator = lineterminator
        self.error_bad_lines = error_bad_lines

        self.gen = gen
        self.vault_name = vault_name
        super().__init__(
            name="adls_to_df",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Load file(s) from the Azure Data Lake to a pandas DataFrame."""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "path",
        "sep",
        "quoting",
        "lineterminator",
        "error_bad_lines",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        path: str = None,
        sep: str = None,
        quoting: int = None,
        lineterminator: str = None,
        error_bad_lines: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> pd.DataFrame:
        """Task run method.

        Args:
            path (str): The path to file(s) which should be loaded into a DataFrame.
            sep (str): The field separator to use when loading the file to the DataFrame.
            quoting (int, optional): The quoting mode to use when reading a CSV file. Defaults to 0.
            lineterminator (str, optional): The newline separator to use when reading a CSV file. Defaults to None.
            error_bad_lines (bool, optional): Whether to raise an exception on bad lines. Defaults to None.
            gen (int): The generation of the Azure Data Lake.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        if quoting is None:
            quoting = 0

        if path is None:
            raise ValueError("Please provide the path to the file to be downloaded.")

        if not sp_credentials_secret:
            # attempt to read a default for the service principal secret name
            try:
                sp_credentials_secret = PrefectSecret(
                    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
                ).run()
            except ValueError:
                pass

        if sp_credentials_secret:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials, path=path)

        full_dl_path = os.path.join(credentials["ACCOUNT_NAME"], path)
        self.logger.info(f"Downloading data from {full_dl_path} to a DataFrame...")
        df = lake.to_df(
            sep=sep,
            quoting=quoting,
            lineterminator=lineterminator,
            error_bad_lines=error_bad_lines,
        )
        self.logger.info(f"Successfully loaded data.")
        return df


class AzureDataLakeCopy(Task):
    """
    Task for copying data between the Azure Data lakes files.

    Args:
        from_path (str, optional): The path from which to copy the file(s). Defaults to None.
        to_path (str, optional): The destination path. Defaults to None.
        recursive (bool, optional): Set this to true if copy entire directories.
        gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
        vault_name (str, optional): The name of the vault from which to fetch the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
        max_retries (int, optional): [description]. Defaults to 3.
        retry_delay (timedelta, optional): [description]. Defaults to timedelta(seconds=10).
    """

    def __init__(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = False,
        gen: int = 2,
        vault_name: str = None,
        timeout: int = 3600,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.from_path = from_path
        self.to_path = to_path
        self.recursive = recursive
        self.gen = gen
        self.vault_name = vault_name

        super().__init__(
            name="adls_copy",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Copy file(s) from the Azure Data Lake"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "from_path",
        "to_path",
        "recursive",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> None:
        """Task run method.

        Args:
            from_path (str): The path from which to copy the file(s).
            to_path (str): The destination path.
            recursive (bool): Set this to true if copying entire directories.
            gen (int): The generation of the Azure Data Lake.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """
        file_name = from_path.split("/")[-1]
        to_path = to_path or file_name

        if not sp_credentials_secret:
            # attempt to read a default for the service principal secret name
            try:
                sp_credentials_secret = PrefectSecret(
                    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
                ).run()
            except ValueError:
                pass

        if sp_credentials_secret:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials)

        full_dl_path = os.path.join(credentials["ACCOUNT_NAME"], from_path)
        self.logger.info(f"Copying data from {full_dl_path} to {to_path}...")
        lake.cp(from_path=from_path, to_path=to_path, recursive=recursive)
        self.logger.info(f"Successfully copied data to {to_path}.")


class AzureDataLakeList(Task):
    """
    Task for listing files in Azure Data Lake.

    Args:
        path (str, optional): The path to the directory which contents you want to list. Defaults to None.
        gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
        vault_name (str, optional): The name of the vault from which to fetch the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
        max_retries (int, optional): [description]. Defaults to 3.
        retry_delay (timedelta, optional): [description]. Defaults to timedelta(seconds=10).

    Returns:
        List[str]: The list of paths to the contents of `path`. These paths
        do not include the container, eg. the path to the file located at
        "https://my_storage_acc.blob.core.windows.net/raw/supermetrics/test_file.txt"
        will be shown as "raw/supermetrics/test_file.txt".
    """

    def __init__(
        self,
        path: str = None,
        gen: int = 2,
        vault_name: str = None,
        timeout: int = 3600,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.path = path
        self.gen = gen
        self.vault_name = vault_name

        super().__init__(
            name="adls_list",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "path",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        path: str = None,
        recursive: bool = False,
        file_to_match: str = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> List[str]:
        """Task run method.

        Args:
            path (str, optional): The path to the directory which contents you want to list. Defaults to None.
            recursive (bool, optional): If True, recursively list all subdirectories and files. Defaults to False.
            file_to_match (str, optional): If exist it only returns files with that name. Defaults to None.
            gen (int): The generation of the Azure Data Lake. Defaults to None.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

        Returns:
            List[str]: The list of paths to the contents of `path`. These paths
            do not include the container, eg. the path to the file located at
            "https://my_storage_acc.blob.core.windows.net/raw/supermetrics/test_file.txt"
            will be shown as "raw/supermetrics/test_file.txt".
        """

        if not sp_credentials_secret:
            # attempt to read a default for the service principal secret name
            try:
                sp_credentials_secret = PrefectSecret(
                    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
                ).run()
            except ValueError:
                pass

        if sp_credentials_secret:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials)

        full_dl_path = os.path.join(credentials["ACCOUNT_NAME"], path)

        self.logger.info(f"Listing files in {full_dl_path}.")
        if recursive:
            self.logger.info("Loading ADLS directories recursively.")
            files = lake.find(path)
            if file_to_match:
                conditions = [file_to_match in item for item in files]
                valid_files = np.array([])
                if any(conditions):
                    index = np.where(conditions)[0]
                    files = list(np.append(valid_files, [files[i] for i in index]))
                else:
                    raise FileExistsError(
                        f"There are not any available file named {file_to_match}."
                    )
        else:
            files = lake.ls(path)

        self.logger.info(f"Successfully listed files in {full_dl_path}.")
        return files


class AzureDataLakeRemove(Task):
    """
    Task for removing objects from Azure Data Lake.

    Args:
        path (str, optional): The path to the directory from which to delete files. Defaults to None.
        recursive (bool): Set this to true if removing entire directories.
        gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
        vault_name (str, optional): The name of the vault from which to fetch the secret. Defaults to None.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
        max_retries (int, optional): Maximum number of retries before failing. Defaults to 3.
        retry_delay (timedelta, optional): Time to wait before the next retry attempt. Defaults to timedelta(seconds=10).
    """

    def __init__(
        self,
        path: str = None,
        recursive: bool = False,
        gen: int = 2,
        vault_name: str = None,
        timeout: int = 3600,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.path = path
        self.recursive = recursive
        self.gen = gen
        self.vault_name = vault_name

        super().__init__(
            name="adls_rm",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "path",
        "recursive",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        path: str = None,
        recursive: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> List[str]:
        """Task run method.

        Args:
            path (str): The path to the directory or file you want to delete. Defaults to None.
            recursive (bool): Set this to True if removing files recursively. Defaults to False.
            gen (int): The generation of the Azure Data Lake. Defaults to None.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        if not sp_credentials_secret:
            # attempt to read a default for the service principal secret name
            try:
                sp_credentials_secret = PrefectSecret(
                    "AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET"
                ).run()
            except ValueError:
                pass

        if sp_credentials_secret:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials)
        full_path = os.path.join(credentials["ACCOUNT_NAME"], path)

        if full_path.endswith(".csv") or full_path.endswith(".parquet"):
            logger_details = "file from"
        else:
            logger_details = "directory"

        self.logger.info(f"Deleting {logger_details} {full_path}...")
        lake.rm(path, recursive=recursive)
        self.logger.info(f"Successfully deleted {logger_details} {full_path}.")
