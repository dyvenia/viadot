import paramiko
import prefect
from typing import Any, Dict, List
from io import StringIO, BytesIO
import prefect
import pandas as pd
from viadot.sources.base import Source
from pathlib import Path
import time
import os
from stat import S_ISDIR
from collections import defaultdict
from ..exceptions import CredentialError
import itertools


class SftpConnector(Source):
    def __init__(
        self,
        file_name: str = None,
        credentials_sftp: Dict[str, Any] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        STFP connector which allows for download data files, listing and downloading into Data Frame.

        Args:
            file_name (str, optional): File name to download. Defaults to None.
            credentials_sftp (Dict[str, Any], optional): SFTP server credentials. Defaults to None.

        Notes:
            self.conn is paramiko.SFTPClient.from_transport method that contains additional methods like
            get, put, open etc. Some of them were not implemented in that class.
            For more check documentation (https://docs.paramiko.org/en/stable/api/sftp.html)

            sftp = SftpConnector()
            sftp.conn.open(filename='folder_a/my_file.zip', mode='r')


        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter.
        """

        self.logger = prefect.context.get("logger")

        self.credentials_sftp = credentials_sftp

        if self.credentials_sftp is None:
            raise CredentialError("Credentials not found.")

        self.file_name = file_name
        self.conn = None
        self.hostname = self.credentials_sftp.get("HOSTNAME")
        self.username = self.credentials_sftp.get("USERNAME")
        self.password = self.credentials_sftp.get("PASSWORD")
        self.port = self.credentials_sftp.get("PORT")
        self.rsa_key = self.credentials_sftp.get("RSA_KEY")

        self.recursive_files_list = {}
        self.file_name_list = []
        self.recursive_files = []

    def get_conn(self) -> paramiko.SFTPClient:
        """Returns a SFTP connection object.

        Returns: paramiko.SFTPClient.
        """

        ssh = paramiko.SSHClient()

        if self.conn is None:
            if self.rsa_key is None:
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

            return self.conn

    def get_cwd(self) -> str:
        """Return the current working directory for SFTP session.

        Returns:
            str: current working directory.
        """
        return self.conn.getcwd()

    def getfo_file(self, file_name: str) -> BytesIO:
        """Copy a remote file from the SFTP server and write to a file-like object.

        Args:
            file_name (str, optional): File name to copy.

        Returns:
            BytesIO: file-like object.
        """
        flo = BytesIO()
        try:
            self.conn.getfo(file_name, flo)
            return flo
        except Exception as e:
            self.logger.info(e)

    def to_df(
        self, file_name: str, sep: str = "\t", columns: List[str] = None, **kwargs
    ) -> pd.DataFrame:
        """Copy a remote file from the SFTP server and write it to Pandas dataframe.

        Args:
            file_name (str, optional): File name to download.
            sep (str, optional): The delimiter for the source file. Defaults to "\t".
            columns (List[str], optional): List of columns to select from file. Defaults to None.

        Returns:
            pd.DataFrame: Pandas dataframe.
        """
        byte_file = self.getfo_file(file_name=file_name)
        byte_file.seek(0)

        if Path(file_name).suffix == ".csv":
            df = pd.read_csv(byte_file, sep=sep, usecols=columns, **kwargs)

        elif Path(file_name).suffix == ".parquet":
            df = pd.read_parquet(byte_file, usecols=columns, **kwargs)

        elif Path(file_name).suffix == ".tsv":
            df = pd.read_csv(byte_file, sep="\t", usecols=columns, **kwargs)

        elif Path(file_name).suffix in [".xls", ".xlsx", ".xlsm"]:
            df = pd.read_excel(byte_file, usecols=columns, **kwargs)

        elif Path(file_name).suffix == ".json":
            df = pd.read_json(byte_file, **kwargs)

        elif Path(file_name).suffix == ".pkl":
            df = pd.read_pickle(byte_file, **kwargs)

        elif Path(file_name).suffix == ".sql":
            df = pd.read_sql(byte_file, **kwargs)

        elif Path(file_name).suffix == ".hdf":
            df = pd.read_hdf(byte_file, **kwargs)

        else:
            raise ValueError(
                f"Not able to read the file {Path(file_name).name}, unsupported filetype: {Path(file_name).suffix}"
            )

        return df

    def get_exported_files(self) -> List[str]:
        """List only exported files in current working directory.

        Returns:
            List: List of exported files.
        """
        self.file_name_list.clear()

        directory_structure = self.conn.listdir()
        for file in directory_structure:
            if "##exported.tsv" in file:
                self.file_name_list.append(file)

        return self.file_name_list

    def list_directory(self, path: str = None) -> List[str]:
        """Returns a list of files on the remote system.

        Args:
            path (str, optional): full path to the remote directory to list. Defaults to None.

        Returns:
            List: List of files.

        """

        if path is None:
            files = self.conn.listdir()
        else:
            files = self.conn.listdir(path)
        return files

    def recursive_listdir(self, path=".", files=None) -> defaultdict(list):
        """Recursively returns a defaultdict of files on the remote system.

        Args:
            path (str, optional): full path to the remote directory to list. Defaults to None.
            files (any, optional): parameter for calling function recursively.
        Returns:
            defaultdict(list): List of files.

        """

        if files is None:
            files = defaultdict(list)

        # loop over list of SFTPAttributes (files with modes)
        for attr in self.conn.listdir_attr(path):

            if S_ISDIR(attr.st_mode):
                # If the file is a directory, recurse it
                self.recursive_listdir(os.path.join(path, attr.filename), files)

            else:
                #  if the file is a file, add it to our dict
                files[path].append(attr.filename)

        self.recursive_files = files
        return files

    def process_defaultdict(self, defaultdict: any = None) -> list:
        """Process defaultdict to list of files.

        Args:
            defaultdict (any, optional): defaultdict of recursive files. Defaults to None.

        Returns:
            List: list of files.
        """
        path_list = []
        if defaultdict is None:
            defaultdict = self.recursive_files

        for item in list(defaultdict.items()):
            tuple_list_path = list(itertools.product([item[0]], item[1]))
            path_list.extend(
                [os.path.join(*tuple_path) for tuple_path in tuple_list_path]
            )
        return path_list

    def close_conn(self) -> None:
        """Closes the connection"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
