import json
import base64
import warnings
import paramiko
import re
import prefect
from typing import Any, Dict, List, Literal
from io import StringIO, BytesIO
import prefect
import pandas as pd
from datetime import datetime, timedelta
from viadot.sources.base import Source
from pathlib import Path
import time
import os
from stat import S_ISDIR, S_ISREG
from collections import defaultdict


class SFTP(Source):
    def __init__(
        self,
        only_export_files: str = True,
        file_name: str = None,
        file_extension: Literal["xls", "xlsx", "csv"] = "csv",
        credentials_sftp: Dict[str, Any] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        STFP connector which allows for download data files, listing and downloading into Data Frame.

        Args:
            only_export_files (str, optional):
            credentials_sftp (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID,

        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter.
        """

        self.logger = prefect.context.get("logger")

        self.credentials_sftp = credentials_sftp

        if self.credentials_sftp is None:
            raise CredentialError("Credentials not found.")

        self.only_export_files = only_export_files
        self.file_name = file_name
        self.file_extension = file_extension

        self.conn = None
        self.hostname = self.credentials_sftp.get("HOSTNAME")
        self.username = self.credentials_sftp.get("USERNAME")
        self.password = self.credentials_sftp.get("PASSWORD")
        self.port = self.credentials_sftp.get("PORT")
        self.rsa_key = self.credentials_sftp.get("RSA_KEY")

        self.recursive_files_list = {}
        self.file_name_list = []

    def get_conn(self):
        """
        Returns an SFTP connection object
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

    def get_cwd(self):
        return self.conn.getcwd()

    def getfo_file(self, file_name: str):
        flo = BytesIO()
        try:
            self.conn.getfo(file_name, flo)
            return flo
        except Exception as e:
            print(e)

    def to_df(self, file_name: str, sep: str = "\t", columns: List[str] = None):

        byte_file = self.getfo_file(file_name=file_name)
        byte_file.seek(0)
        if columns is None:
            if Path(file_name).suffix == ".csv":
                df = pd.read_csv(byte_file, sep=sep)

            elif Path(file_name).suffix == ".parquet":
                df = pd.read_parquet(byte_file)
        if columns is not None:
            if Path(file_name).suffix == ".csv":
                df = pd.read_csv(byte_file, sep=sep, usecols=columns)

            elif Path(file_name).suffix == ".parquet":
                df = pd.read_parquet(byte_file, usecols=columns)

        return df

    def get_exported_files(self, return_data: bool = False):

        self.file_name_list.clear()

        directory_structure = self.conn.listdir()
        for file in directory_structure:
            if "##exported.tsv" in file:
                self.file_name_list.append(file)

        return self.file_name_list

    def list_directory(self, path: str = None) -> List[str]:
        """
        Returns a list of files on the remote system.

        Args:
            path (str, optional): full path to the remote directory to list. Defaults to None.

        """
        # conn = self.get_conn()
        if path is None:
            files = self.conn.listdir()
        else:
            files = self.conn.listdir(path)
        return files

    def recursive_ftp(self, path=".", files=None):
        if files is None:
            files = defaultdict(list)

        # loop over list of SFTPAttributes (files with modes)
        for attr in self.conn.listdir_attr(path):

            if S_ISDIR(attr.st_mode):
                # If the file is a directory, recurse it
                self.recursive_ftp(os.path.join(path, attr.filename), files)

            else:
                #  if the file is a file, add it to our dict
                files[path].append(attr.filename)

        return files

    def close_conn(self) -> None:
        """
        Closes the connection
        """
        if self.conn is not None:
            self.conn.close()
            self.conn = None
