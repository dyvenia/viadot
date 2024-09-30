"""SFTP connector."""

from io import BytesIO, StringIO
from pathlib import Path
import re
from stat import S_ISDIR, S_ISREG
import time
from typing import Literal

import pandas as pd
import paramiko
from paramiko.sftp import SFTPError
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class SftpCredentials(BaseModel):
    """Checking for values in SFTP credentials dictionary.

    Two key values are held in the Salesforce connector:
        - hostname: IP address of the SFTP server..
        - username: The user name for SFTP connection.
        - password: The passwrod for SFTP connection.
        - port: The port to use for the connection.
        - rsa_key: The SSH key to use for the connection. Only RSA is currently
            supported.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    hostname: str
    username: str
    password: str
    port: int
    rsa_key: str | None


class Sftp(Source):
    """Class implementing a SFTP server connection."""

    def __init__(
        self,
        *args,
        credentials: SftpCredentials | None = None,
        config_key: str = "sftp",
        **kwargs,
    ):
        """Create an instance of SFTP.

        Args:
            credentials (SftpCredentials, optional): SFTP credentials. Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "sftp".

        Notes:
            self.conn is paramiko.SFTPClient.from_transport method that contains
            additional methods like get, put, open etc. Some of them were not
            implemented in that class. For more check documentation
            (https://docs.paramiko.org/en/stable/api/sftp.html).

            sftp = Sftp()
            sftp.conn.open(filename='folder_a/my_file.zip', mode='r')

        Raises:
            CredentialError: If credentials are not provided in viadot config or
                directly as a parameter.
        """
        credentials = credentials or get_source_credentials(config_key)

        if credentials is None:
            message = "Missing credentials."
            raise CredentialError(message)

        validated_creds = dict(SftpCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.conn = None
        self.hostname = validated_creds.get("hostname")
        self.username = validated_creds.get("username")
        self.password = validated_creds.get("password")
        self.port = validated_creds.get("port")
        self.rsa_key = validated_creds.get("rsa_key")

    def _get_file_object(self, file_name: str) -> BytesIO:
        """Copy a remote file from the SFTP server and write to a file-like object.

        Args:
            file_name (str, optional): File name to copy.

        Returns:
            BytesIO: file-like object.
        """
        file_object = BytesIO()
        try:
            self.conn.getfo(file_name, file_object)

        except FileNotFoundError as error:
            raise SFTPError from error

        else:
            return file_object

    def get_connection(self) -> paramiko.SFTPClient:
        """Returns a SFTP connection object.

        Returns: paramiko.SFTPClient.
        """
        ssh = paramiko.SSHClient()

        if not self.rsa_key:
            transport = paramiko.Transport((self.hostname, self.port))
            transport.connect(None, self.username, self.password)

            self.conn = paramiko.SFTPClient.from_transport(transport)

        else:
            mykey = paramiko.RSAKey.from_private_key(StringIO(self.rsa_key))
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # noqa: S507
            ssh.connect(self.hostname, username=self.username, pkey=mykey)
            time.sleep(1)
            self.conn = ssh.open_sftp()

        self.logger.info("Connected to the SFTP server.")

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        file_name: str | None = None,
        sep: str = "\t",
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        r"""Copy a remote file from the SFTP server and write it to Pandas dataframe.

        Args:
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if
                the fetch produces no data. Defaults to "warn".
            file_name (str, optional): The name of the file to download.
            sep (str, optional): The delimiter for the source file. Defaults to "\t".
            columns (list[str], optional): List of columns to select from file.
                Defaults to None.

        Returns:
            pd.DataFrame: The response data as a Pandas Data Frame plus viadot metadata.
        """
        byte_file = self._get_file_object(file_name=file_name)
        byte_file.seek(0)

        self._close_conn()

        suffix = Path(file_name).suffix
        if suffix == ".csv":
            df = pd.read_csv(byte_file, sep=sep, usecols=columns)

        elif suffix == ".parquet":
            df = pd.read_parquet(byte_file, usecols=columns)

        elif suffix == ".tsv":
            df = pd.read_csv(byte_file, sep=sep, usecols=columns)

        elif suffix in [".xls", ".xlsx", ".xlsm"]:
            df = pd.read_excel(byte_file, usecols=columns)

        elif suffix == ".json":
            df = pd.read_json(byte_file)

        elif suffix == ".pkl":
            df = pd.read_pickle(byte_file)  # noqa: S301

        elif suffix == ".sql":
            df = pd.read_sql(byte_file)

        elif suffix == ".hdf":
            df = pd.read_hdf(byte_file)

        else:
            message = (
                f"Unable to read file '{Path(file_name).name}', "
                + f"unsupported filetype: {suffix}"
            )
            raise ValueError(message)

        if df.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info("Successfully downloaded data from the SFTP server.")

        return df

    def _ls(self, path: str | None = ".", recursive: bool = False) -> list[str]:
        """List files in specified directory, with optional recursion.

        Args:
            path (str | None): Full path to the remote directory to list.
                Defaults to ".".
            recursive (bool): Whether to list files recursively. Defaults to False.

        Returns:
            list[str]: List of files in the specified directory.
        """
        files_list = []

        path = "." if path is None else path
        try:
            if not recursive:
                return [
                    str(Path(path) / attr.filename)
                    for attr in self.conn.listdir_attr(path)
                    if S_ISREG(attr.st_mode)
                ]

            for attr in self.conn.listdir_attr(path):
                full_path = str(Path(path) / attr.filename)
                if S_ISDIR(attr.st_mode):
                    files_list.extend(self._ls(full_path, recursive=True))
                else:
                    files_list.append(full_path)
        except FileNotFoundError as e:
            self.logger.info(f"Directory not found: {path}. Error: {e}")
        except Exception as e:
            self.logger.info(f"Error accessing {path}: {e}")

        return files_list

    def get_files_list(
        self,
        path: str | None = None,
        recursive: bool = False,
        matching_path: str | None = None,
    ) -> list[str]:
        """List files in `path`.

        Args:
            path (str | None): Destination path from where to get the structure.
                Defaults to None.
            recursive (bool): Get the structure in deeper folders.
                Defaults to False.
            matching_path (str | None): Filtering folders to return by a regex
                pattern. Defaults to None.

        Returns:
            list[str]: List of files in the specified path.
        """
        files_list = self._ls(path=path, recursive=recursive)

        if matching_path is not None:
            files_list = [f for f in files_list if re.match(matching_path, f)]

        self._close_conn()

        self.logger.info("Successfully loaded file list from SFTP server.")

        return files_list

    def _close_conn(self) -> None:
        """Close the SFTP server connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
