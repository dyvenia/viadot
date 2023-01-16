from typing import Any, Dict

import sharepy

import os
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential

from ..config import local_config
from ..exceptions import CredentialError
from .base import Source


class Sharepoint(Source):
    """
    A Sharepoint class to connect and download specific Excel file from Sharepoint.

    Args:
        credentials (dict): In credentials should be included:
            "site" - Path to sharepoint website (e.g : {tenant_name}.sharepoint.com)
            "username" - Sharepoint username (e.g username@{tenant_name}.com)
            "password"
        download_from_path (str, optional): Full url to file
                        (e.g : https://{tenant_name}.sharepoint.com/sites/{directory}/Shared%20Documents/Dashboard/file). Defaults to None.
    """

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        download_from_path: str = None,
        *args,
        **kwargs,
    ):

        DEFAULT_CREDENTIALS = local_config.get("SHAREPOINT")
        credentials = credentials or DEFAULT_CREDENTIALS
        if credentials is None:
            raise CredentialError("Credentials not found.")
        self.url = download_from_path
        self.required_credentials = ["site", "username", "password"]
        super().__init__(*args, credentials=credentials, **kwargs)

    def get_connection(self) -> sharepy.session.SharePointSession:
        if any([rq not in self.credentials for rq in self.required_credentials]):
            raise CredentialError("Missing credentials.")

        return sharepy.connect(
            site=self.credentials["site"],
            username=self.credentials["username"],
            password=self.credentials["password"],
        )

    def download_file(
        self,
        download_from_path: str = None,
        download_to_path: str = "Sharepoint_file.xlsm",
    ) -> None:
        """Function to download files from Sharepoint.
        Args:
            download_from_path (str): Path from which to download file. Defaults to None.
            download_to_path (str, optional): Path to destination file. Defaults to "Sharepoint_file.xlsm".
        """
        download_from_path = download_from_path or self.url
        if not download_from_path:
            raise ValueError("Missing required parameter 'download_from_path'.")

        conn = self.get_connection()
        conn.getfile(
            url=download_from_path,
            filename=download_to_path,
        )

    def download_file_from_url(
        self,
        rel_file_url,
        download_folder: str = "./",
        file_name: str = "Sharepoint_file.xlsm",
    ) -> None:
        """Function to download a file from sharepoint, given its URL
        Args:
            abs_url (str): URL to sharepoint website (e.g.: {tenant_name}.sharepoint.com)
            rel_file_url (str): Relative URL to the file (e.g.: /sites/{directory}/Shared%20Documents/Downloads/)
            credentials (str,str): Tuple with user and password (for now)
            download_folder (str): Path where the file will be downloaded to

        """
        abs_url = self.credentials["site"]
        rel_file_url = rel_file_url or self.url
        user_name = self.credentials["username"]
        user_password = self.credentials["password"]

        abs_file_url = str(abs_url + rel_file_url)

        with open(os.path.join(download_folder, file_name), "wb") as local_file:
            file = (
                File.from_url(abs_file_url)
                .with_credentials(UserCredential(user_name, user_password))
                .download(local_file)
                .execute_query()
            )
