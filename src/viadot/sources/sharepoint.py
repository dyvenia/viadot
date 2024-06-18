import io
from typing import Literal, Optional, Union
from urllib.parse import urlparse
import os

import pandas as pd
import sharepy
from pydantic import BaseModel, root_validator
from sharepy.errors import AuthError

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.signals import SKIP
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, cleanup_df, validate


class SharepointCredentials(BaseModel):
    site: str  # Path to sharepoint website (e.g : {tenant_name}.sharepoint.com)
    username: str  # Sharepoint username (e.g username@{tenant_name}.com)
    password: str  # Sharepoint password

    @root_validator(pre=True)
    def is_configured(cls, credentials):
        site = credentials.get("site")
        username = credentials.get("username")
        password = credentials.get("password")

        if not (site and username and password):
            raise CredentialError(
                "'site', 'username', and 'password' credentials are required."
            )
        return credentials


def get_last_segment_from_url(
    url: str,
) -> tuple[str, Literal["file"]] | tuple[str, Literal["directory"]]:
    """
    Get the last part of the URL and determine if it represents a file or directory.

    This function parses the provided URL, extracts the last segment, and identifies
    whether it corresponds to a file (based on the presence of a file extension)
    or a directory.

    Args:
        url (str): The URL to a SharePoint file or directory.

    Raises:
        ValueError: If an invalid URL is provided.

    Returns:
        tuple: A tuple where the first element is the last part of the URL (file extension
        or folder name) and the second element is a string indicating the type:
            - If a file URL is provided, returns (file extension, 'file').
            - If a folder URL is provided, returns (last folder name, 'directory').
    """
    path_parts = urlparse(url).path.split("/")
    # Filter out empty parts
    non_empty_parts = [part for part in path_parts if part]

    # Check if the last part has a file extension
    if non_empty_parts:
        last_part = non_empty_parts[-1]
        _, extension = os.path.splitext(last_part)
        if extension:
            return extension, "file"
        else:
            return last_part, "directory"
    else:
        raise ValueError("Incorrect URL provided : '{url}'")


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
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(
            SharepointCredentials(**raw_creds)
        )  # validate the credentials
        super().__init__(*args, credentials=validated_creds, **kwargs)

    def get_connection(self) -> sharepy.session.SharePointSession:
        try:
            connection = sharepy.connect(
                site=self.credentials.get("site"),
                username=self.credentials.get("username"),
                password=self.credentials.get("password"),
            )
        except AuthError:
            site = self.credentials.get("site")
            raise CredentialError(
                f"Could not authenticate to {site} with provided credentials."
            )
        return connection

    def download_file(self, url: str, to_path: list | str) -> None:
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

    @add_viadot_metadata_columns
    def to_df(
        self,
        url: str,
        sheet_name: Optional[Union[str, list, int]] = None,
        if_empty: str = "warn",
        tests: dict = {},
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load an Excel file into a pandas DataFrame.

        Args:
            url (str): The URL of the file to be downloaded.
            sheet_name (Optional[Union[str, list, int]], optional): Strings are used for sheet names.
                Integers are used in zero-indexed sheet positions (chart sheets do not count
                as a sheet position). Lists of strings/integers are used to request multiple sheets.
                Specify None to get all worksheets. Defaults to None.
            if_empty (str, optional): What to do if the file is empty. Defaults to "warn".
            tests (Dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from utils. Defaults to None.
            kwargs (dict[str, Any], optional): Keyword arguments to pass to pd.ExcelFile.parse(). Note that
            `nrows` is not supported.

        Returns:
            pd.DataFrame: The resulting data as a pandas DataFrame.
        """
        endpoint_value, endpoint_type = get_last_segment_from_url(url)

        if "nrows" in kwargs:
            raise ValueError("Parameter 'nrows' is not supported.")

        conn = self.get_connection()
        ## add option to get only excel files - if needed - here folder with only excels is required
        if endpoint_type == "file":
            if endpoint_value != ".xlsx":
                raise ValueError(
                    "Only Excel files with 'XLSX' extension can be loaded into a DataFrame."
                )
            self.logger.info(f"Downloading data from {url}...")

            response = conn.get(url)
            bytes_stream = io.BytesIO(response.content)
            excel_file = pd.ExcelFile(bytes_stream)
            na_values = [
                "",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "1.#IND",
                "1.#QNAN",
                "N/A",
                "NULL",
                "NaN",
                "n/a",
                "nan",
                None,
            ]

            if sheet_name:
                df = excel_file.parse(
                    sheet_name=sheet_name,
                    keep_default_na=False,
                    na_values=na_values,
                    **kwargs,
                )
                df["sheet_name"] = sheet_name
            else:
                sheets: list[pd.DataFrame] = []
                for sheet_name in excel_file.sheet_names:
                    sheet = excel_file.parse(
                        sheet_name=sheet_name,
                        keep_default_na=False,
                        na_values=na_values,
                        **kwargs,
                    )
                    sheet["sheet_name"] = sheet_name
                    sheets.append(sheet)
                df = pd.concat(sheets)

            if df.empty:
                try:
                    self._handle_if_empty(if_empty)
                except SKIP:
                    return pd.DataFrame()
            else:
                self.logger.info(f"Successfully downloaded {len(df)} of data.")

            df_clean = cleanup_df(df)

            if tests:
                validate(df=df_clean, tests=tests)

            return df_clean.astype(str).where(pd.notnull(df_clean), None)
