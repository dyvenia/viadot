from pandas._config import config
import sharepy

from .base import Source
from ..config import local_config


class Sharepoint(Source):
    def __init__(
        self,
        site: str = None,
        username: str = None,
        password: str = None,
        url_to_file: str = None,
        *args,
        **kwargs
    ):
        """
        A Sharepoint class to connect and download specific Excel file from Sharepoint.

        Args:
                site (str, optional): Path to sharepoint website (e.g : {tenant_name}.sharepoint.com). Defaults to None.
                username (str, optional): Sharepoint username (e.g username@{tenant_name}.com). Defaults to None.
                password (str, optional): Sharepoint password. Defaults to None.
                url_to_file (str, optional): Full url to file
                                (e.g : https://{tenant_name}.sharepoint.com/sites/{folder}/Shared%20Documents/Dashboard/{file_name}). Defaults to None.
        """
        credentials = local_config.get("SHAREPOINT")
        self.site = site or credentials["site"]
        self.username = username or credentials["username"]
        self.password = password or credentials["password"]
        self.url_to_file = url_to_file or credentials["file_url"]

        super().__init__(*args, **kwargs)

    def get_connection(self) -> sharepy.session.SharePointSession:
        return sharepy.connect(
            site=self.site, username=self.username, password=self.password
        )

    def download_file(
        self, url_to_file: str = None, filename: str = "Sharepoint_file.xlsm"
    ) -> None:
        conn = self.get_connection()
        conn.getfile(url=url_to_file or self.url_to_file, filename=filename)
