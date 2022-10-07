from typing import Optional

import sharepy
from pydantic import BaseModel

from ..config import DEFAULT_CONFIG
from .base import Source


class SharepointCredentials(BaseModel):
    site: str  # Path to sharepoint website (e.g : {tenant_name}.sharepoint.com)
    username: str  # Sharepoint username (e.g username@{tenant_name}.com)
    password: str  # Sharepoint password


class Sharepoint(Source):
    """
    Download Excel files from Sharepoint.

    Args:
        credentials (SharepointCredentials): Sharepoint credentials.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
    """

    def __init__(
        self,
        credentials: SharepointCredentials = None,
        config_key: Optional[str] = None,
        *args,
        **kwargs,
    ):
        credentials = credentials or DEFAULT_CONFIG.get(config_key)
        SharepointCredentials(**credentials)  # validate the credentials schema
        super().__init__(*args, credentials=credentials, **kwargs)

    def get_connection(self) -> sharepy.session.SharePointSession:
        return sharepy.connect(
            site=self.credentials["site"],
            username=self.credentials["username"],
            password=self.credentials["password"],
        )

    def download_file(
        self,
        from_path: str,
        to_path: str,
    ) -> None:
        """
        Download a file from Sharepoint.

        Args:
            from_path (str): The URL of the file.
            to_path (str): Where to download the file.

        Example:
            download_file(
                from_path="https://{tenant_name}.sharepoint.com/sites/{directory}/Shared%20Documents/Dashboard/file",
                to_path="file.xlsx"
            )
        """
        conn = self.get_connection()
        conn.getfile(
            url=from_path,
            filename=to_path,
        )
        conn.close()
