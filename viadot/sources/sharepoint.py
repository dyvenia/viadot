import io
from typing import Optional, Union

import pandas as pd
import sharepy
from pydantic import BaseModel
from sharepy.errors import AuthError
from viadot.exceptions import CredentialError

from ..config import get_source_credentials
from ..utils import handle_if_empty
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
        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            raise CredentialError("Please specify the credentials.")
        SharepointCredentials(**credentials)  # validate the credentials schema
        super().__init__(*args, credentials=credentials, **kwargs)
        self.logger.info(credentials)

    def get_connection(self) -> sharepy.session.SharePointSession:
        try:
            connection = sharepy.connect(
                site=self.credentials["site"],
                username=self.credentials["username"],
                password=self.credentials["password"],
            )
        except AuthError:
            raise CredentialError(
                f"Could not authenticate to {self.credentials.site} with provided credentials."
            )

        return connection

    def download_file(
        self,
        url: str,
        to_path: str,
    ) -> None:
        """
        Download a file from Sharepoint.

        Args:
            url (str): The URL of the file to be downloaded.
            to_path (str): Where to download the file.

        Example:
            download_file(
                url="https://{tenant_name}.sharepoint.com/sites/{directory}/Shared%20Documents/Dashboard/file",
                to_path="file.xlsx"
            )
        """
        conn = self.get_connection()
        conn.getfile(
            url=url,
            filename=to_path,
        )
        conn.close()

    def to_df(
        self,
        url: str,
        sheet_name: Optional[Union[str, list, int]] = None,
        if_empty: str = "warn",
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load an Excel file into a pandas DataFrame.

        Args:
            url (str): The URL of the file to be downloaded.
            sheet_name (Optional[Union[str, list, int]], optional): The name of the sheet to download. Defaults to None.
            if_empty (str, optional): What to do if the file is empty. Defaults to "warn".
            kwargs (dict[str, Any], optional): Keyword arguments to pass to pd.ExcelFile.parse(). Note that
            `nrows` is not supported.

        Returns:
            pd.DataFrame: The resulting data as a pandas DataFrame.
        """

        if "xls" not in url.split(".")[-1]:
            raise ValueError("Only Excel files can be loaded into a DataFrame.")

        if "nrows" in kwargs:
            raise ValueError("Parameter 'nrows' is not supported.")

        conn = self.get_connection()

        self.logger.info(f"Downloading data from {url}...")

        response = conn.get(url)
        bytes_stream = io.BytesIO(response.content)
        excel_file = pd.ExcelFile(bytes_stream)

        if sheet_name:
            df = excel_file.parse(sheet_name, **kwargs)
        else:
            sheets: list[pd.DataFrame] = []
            for sheet_name in excel_file.sheet_names:
                sheet = excel_file.parse(sheet_name=sheet_name, **kwargs)
                sheet["sheet_name"] = sheet_name
                sheets.append(sheet)
            df = pd.concat(sheets)

        self.logger.info(f"Successfully downloaded {len(df)} of data.")

        if df.empty:
            handle_if_empty(if_empty)

        return df
