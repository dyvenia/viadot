"""SFTP connector."""

import itertools
import time
from collections import defaultdict
from io import BytesIO, StringIO
from pathlib import Path
from stat import S_ISDIR

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
        - hostname: The direction for the SFTP.
        - username: The unique name for SFTP connection.
        - password: The unique passwrod for SFTP connection.
        - port: Number from which the connection will be done.
        - rsa_key: The Company RSA Key.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    hostname: str
    username: str
    password: str
    port: int
    rsa_key: str


class SftpConnector(Source):
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

            sftp = SftpConnector()
            sftp.conn.open(filename='folder_a/my_file.zip', mode='r')

        Raises:
            CredentialError: If credentials are not provided in local_config or
                directly as a parameter.
        """
        credentials = credentials or get_source_credentials(config_key) or None

        if credentials is None:
            message = "Missing credentials."
            raise CredentialError(message)
        self.credentials = credentials

        validated_creds = dict(SftpCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.conn = None
        self.hostname = validated_creds.get("hostname")
        self.username = validated_creds.get("username")
        self.password = validated_creds.get("password")
        self.port = validated_creds.get("port")
        self.rsa_key = validated_creds.get("rsa_key")

    def _getfo_file(self, file_name: str) -> BytesIO:
        """Copy a remote file from the SFTP server and write to a file-like object.

        Args:
            file_name (str, optional): File name to copy.

        Returns:
            BytesIO: file-like object.
        """
        flo = BytesIO()
        try:
            self.conn.getfo(file_name, flo)

        except SFTPError as error:
            self.logger.info(error)

        else:
            return flo

    def get_connection(self) -> paramiko.SFTPClient:
        """Returns a SFTP connection object.

        Returns: paramiko.SFTPClient.
        """
        ssh = paramiko.SSHClient()

        if len(self.rsa_key) == 0:
            transport = paramiko.Transport((self.hostname, self.port))
            transport.connect(None, self.username, self.password)

            self.conn = paramiko.SFTPClient.from_transport(transport)

        else:
            keyfile = StringIO(self.rsa_key)
            mykey = paramiko.RSAKey.from_private_key(keyfile)
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.hostname, username=self.username, pkey=mykey)
            time.sleep(1)
            self.conn = ssh.open_sftp()

        self.logger.info("Connected to the SFTP server.")

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
        file_name: str | None = None,
        sep: str = "\t",
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        r"""Copy a remote file from the SFTP server and write it to Pandas dataframe.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn
            file_name (str, optional): File name to download.
            sep (str, optional): The delimiter for the source file. Defaults to "\t".
            columns (list[str], optional): List of columns to select from file.
                Defaults to None.

        Returns:
            pd.DataFrame: The response data as a Pandas Data Frame plus viadot metadata.
        """
        byte_file = self._getfo_file(file_name=file_name)
        byte_file.seek(0)

        self._close_conn()

        if Path(file_name).suffix == ".csv":
            df = pd.read_csv(byte_file, sep=sep, usecols=columns)

        elif Path(file_name).suffix == ".parquet":
            df = pd.read_parquet(byte_file, usecols=columns)

        elif Path(file_name).suffix == ".tsv":
            df = pd.read_csv(byte_file, sep=sep, usecols=columns)

        elif Path(file_name).suffix in [".xls", ".xlsx", ".xlsm"]:
            df = pd.read_excel(byte_file, usecols=columns)

        elif Path(file_name).suffix == ".json":
            df = pd.read_json(byte_file)

        elif Path(file_name).suffix == ".pkl":
            df = pd.read_pickle(byte_file)

        elif Path(file_name).suffix == ".sql":
            df = pd.read_sql(byte_file)

        elif Path(file_name).suffix == ".hdf":
            df = pd.read_hdf(byte_file)

        else:
            message = (
                f"Not able to read the file {Path(file_name).name}, "
                + f"unsupported filetype: {Path(file_name).suffix}"
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

    def _list_directory(self, path: str | None = None) -> list[str]:
        """Returns a list of files on the remote system.

        Args:
            path (str, optional): full path to the remote directory to list.
                Defaults to None.

        Returns:
            List: List of files.
        """
        path = "." if path is None else path

        return self.conn.listdir(path)

    def _recursive_listdir(
        self, path: str = ".", files: defaultdict(list) = None
    ) -> defaultdict(list):
        """Recursively returns a defaultdict of files on the remote system.

        Args:
            path (str, optional): full path to the remote directory to list.
                Defaults to None.
            files (defaultdict(list), optional): parameter to call recursively.

        Returns:
            defaultdict(list): List of files.
        """
        if files is None:
            files = defaultdict(list)

        for attr in self.conn.listdir_attr(str(path)):
            if S_ISDIR(attr.st_mode):
                self._recursive_listdir(Path(path) / attr.filename, files)

            else:
                files[path].append(attr.filename)

        return files

    def _process_defaultdict(self, defaultdict: defaultdict(list)) -> list[str]:
        """Process defaultdict to list of files.

        Args:
            defaultdict (defaultdict(list)): defaultdict of recursive files.
                Defaults to None.

        Returns:
            List: list of files.
        """
        path_list = []
        for item in list(defaultdict.items()):
            tuple_list_path = list(itertools.product([item[0]], item[1]))

            path_list.extend([str(Path(*tuple_path)) for tuple_path in tuple_list_path])

        return path_list

    def get_files_list(
        self,
        path: str | None = None,
        recursive: bool = False,
        matching_path: str | None = None,
    ) -> defaultdict(list):
        """Get the file `path` structure in the SFTP server.

        Args:
            path (str, optional): Destination path from where to get the structure.
                Defaults to None.
            recursive (bool, optional): Get the structure in deeper folders.
                Defaults to False.
            matching_path (str, optional): Filtering folders to return.
                Defaults to None.

        Returns:
            files_list (defaultdict(list)): List of files in the SFTP server.
        """
        if recursive is False:
            files_list = self._list_directory(path=path)

        else:
            files_list = self._recursive_listdir(path=path)
            files_list = self._process_defaultdict(defaultdict=files_list)

        self._close_conn()

        if matching_path is not None:
            files_list = [f for f in files_list if matching_path in f]

        self.logger.info("Succefully loaded file list from SFTP server.")

        return files_list

    def _close_conn(self) -> None:
        """Close the SFTP server connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
