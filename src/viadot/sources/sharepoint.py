"""Sharepoint API connector."""

from http import HTTPStatus
import io
from pathlib import Path
import re
from typing import Any, Literal
from urllib.parse import urlparse

import pandas as pd
from pandas._libs.parsers import STR_NA_VALUES
from pydantic import BaseModel, root_validator
import requests
import sharepy
from sharepy.errors import AuthError

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.signals import SKIP
from viadot.sources.base import Source
from viadot.utils import (
    add_viadot_metadata_columns,
    cleanup_df,
    validate,
    validate_and_reorder_dfs_columns,
)


class SharepointCredentials(BaseModel):
    site: str  # Path to sharepoint website (e.g : {tenant_name}.sharepoint.com)
    username: str  # Sharepoint username (e.g username@{tenant_name}.com)
    password: str  # Sharepoint password

    @root_validator(pre=True)
    def is_configured(cls, credentials: dict):  # noqa: N805, ANN201, D102
        site = credentials.get("site")
        username = credentials.get("username")
        password = credentials.get("password")

        if not (site and username and password):
            msg = "'site', 'username', and 'password' credentials are required."
            raise CredentialError(msg)
        return credentials


class Sharepoint(Source):
    DEFAULT_NA_VALUES = tuple(STR_NA_VALUES)

    def __init__(
        self,
        credentials: SharepointCredentials = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Download Excel files from Sharepoint.

        Args:
        credentials (SharepointCredentials): Sharepoint credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
        """
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(SharepointCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)

    def get_connection(self) -> sharepy.session.SharePointSession:
        """Establishe a connection to SharePoint.

        Returns:
            sharepy.session.SharePointSession: A session object representing
                the authenticated connection.

        Raises:
            CredentialError: If authentication to SharePoint fails due to incorrect
                credentials.
        """
        try:
            connection = sharepy.connect(
                site=self.credentials.get("site"),
                username=self.credentials.get("username"),
                password=self.credentials.get("password"),
            )
        except AuthError as e:
            site = self.credentials.get("site")
            msg = f"Could not authenticate to {site} with provided credentials."
            raise CredentialError(msg) from e
        return connection

    def download_file(self, url: str, to_path: list | str) -> None:
        """Download a file from Sharepoint to specific location.

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

    def scan_sharepoint_folder(self, url: str) -> list[str]:
        """Scan Sharepoint folder to get all file URLs of all files within it.

        Args:
            url (str): The URL of the folder to scan.

        Raises:
            ValueError: If the provided URL does not contain the expected '/sites/'
                segment.

        Returns:
            list[str]: List of URLs pointing to each file within the specified
                SharePoint folder.
        """
        conn = self.get_connection()

        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split("/")
        if "sites" in path_parts:
            site_index = (
                path_parts.index("sites") + 2
            )  # +2 to include 'sites' and the next segment
            site_url = f"{parsed_url.scheme}://{parsed_url.netloc}{'/'.join(path_parts[:site_index])}"
            library = "/".join(path_parts[site_index:])
        else:
            message = "URL does not contain '/sites/' segment."
            raise ValueError(message)

        # -> site_url = company.sharepoint.com/sites/site_name/
        # -> library = /shared_documents/folder/sub_folder/final_folder
        endpoint = (
            f"{site_url}/_api/web/GetFolderByServerRelativeUrl('{library}')/Files"
        )
        response = conn.get(endpoint)
        files = response.json().get("d", {}).get("results", [])

        return [f"{site_url}/{library}{file['Name']}" for file in files]

    def _get_file_extension(self, url: str) -> str:
        """Extracts the file extension from a given URL.

        Parameters:
            url (str): The URL from which to extract the file extension.

        Returns:
            str: The file extension, including the leading dot (e.g., '.xlsx').
        """
        # Parse the URL to get the path
        parsed_url = urlparse(url)
        return Path(parsed_url.path).suffix

    def _download_file_stream(self, url: str, **kwargs) -> pd.ExcelFile:
        """Download the contents of a file from SharePoint.

        Returns the data as an in-memory byte stream.

        Args:
            url (str): The URL of the file to download.

        Returns:
            io.BytesIO: An in-memory byte stream containing the file content.
        """
        if "nrows" in kwargs:
            msg = "Parameter 'nrows' is not supported."
            raise ValueError(msg)

        conn = self.get_connection()

        self.logger.info(f"Downloading data from {url}...")
        try:
            response = conn.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == HTTPStatus.FORBIDDEN:
                self.logger.exception(f"Access denied to file: {url}")
            else:
                self.logger.exception(f"HTTP error when accessing {url}")
            raise
        except Exception:
            self.logger.exception(f"Failed to download file: {url}")
            raise

        bytes_stream = io.BytesIO(response.content)

        try:
            return pd.ExcelFile(bytes_stream)
        except ValueError:
            self.logger.exception(f"Invalid Excel file: {url}")
            raise

    def _is_file(self, url: str) -> bool:
        """Determines whether a provided URL points to a file based on its structure.

        This function uses a regular expression to check if the URL ends with a
        common file extension. It does not make any network requests and purely
        relies on the URL structure for its determination.

        Parameters:
        url (str): The URL to be checked.

        Returns:
        bool: True if the URL is identified as a file based on its extension,
            False otherwise.

        Example:
        >>> _is_file("https://example.com/file.xlsx")
        True
        >>> _is_file("https://example.com/folder/")
        False
        >>> _is_file("https://example.com/folder")
        False
        """
        # Regular expression for matching file extensions
        file_extension_pattern = re.compile(r"\.[a-zA-Z0-9]+$")

        return bool(file_extension_pattern.search(url))

    def _handle_multiple_files(
        self,
        url: str,
        file_sheet_mapping: dict,
        na_values: list[str] | None = None,
        **kwargs,
    ):
        """Handle downloading and parsing multiple Excel files from a SharePoint folder.

        Args:
            url (str): The base URL of the SharePoint folder containing the files.
            file_sheet_mapping (dict): A dictionary mapping file names to sheet names
                or indexes. The keys are file names, and the values are sheet
                names/indices.
            na_values (Optional[list[str]]): Additional strings to recognize as NA/NaN.

        Returns:
            pd.DataFrame: A concatenated DataFrame containing the data from all
                specified files and sheets.

        Raises:
            ValueError: If the file extension is not supported.
        """
        dfs = []
        for file, sheet in file_sheet_mapping.items():
            file_url = url + file
            try:
                df = self._load_and_parse(
                    file_url=file_url, sheet_name=sheet, na_values=na_values, **kwargs
                )
                dfs.append(df)
            except Exception:
                self.logger.exception(f"Failed to load file: {file_url}")
                continue
        if not dfs:
            self.logger.warning("No valid Excel files were loaded.")
            return pd.DataFrame()
        return pd.concat(validate_and_reorder_dfs_columns(dfs))

    def _load_and_parse(
        self,
        file_url: str,
        sheet_name: str | list[str] | None = None,
        na_values: list[str] | None = None,
        **kwargs,
    ):
        """Loads and parses an Excel file from a URL.

        Args:
            file_url (str): The URL of the file to download and parse.
            sheet_name (Optional[Union[str, list[str]]]): The name(s) or index(es) of
                the sheet(s) to parse. If None, all sheets are parsed.
            na_values (Optional[list[str]]): Additional strings to recognize as NA/NaN.
            **kwargs: Additional keyword arguments to pass to the pandas read function.

        Returns:
            pd.DataFrame: The parsed data as a pandas DataFrame.

        Raises:
            ValueError: If the file extension is not supported.
        """
        file_extension = self._get_file_extension(file_url)
        if file_extension not in [".xlsx", ".xlsm", ".xls"]:
            self.logger.error(
                f"Unsupported file extension: {file_extension} for file: {file_url}"
            )
            msg = (
                "Only Excel (.xlsx, .xlsm, .xls) files can be loaded into a DataFrame."
            )
            raise ValueError(msg)
        file_stream = self._download_file_stream(file_url)
        return self._parse_excel(file_stream, sheet_name, na_values, **kwargs)

    def _parse_excel(
        self,
        excel_file: pd.ExcelFile,
        sheet_name: str | list[str] | None = None,
        na_values: list[str] | None = None,
        **kwargs,
    ):
        """Parses an Excel file into a DataFrame. Casts all columns to string.

        Args:
            excel_file: An ExcelFile object containing the data to parse.
            sheet_name (Optional[Union[str, list[str]]]): The name(s) or index(es) of
                the sheet(s) to parse. If None, all sheets are parsed.
            na_values (Optional[list[str]]): Additional strings to recognize as NA/NaN.
            **kwargs: Additional keyword arguments to pass to the pandas read function.

        Returns:
            pd.DataFrame: The parsed data as a pandas DataFrame.
        """
        return pd.concat(
            [
                excel_file.parse(
                    sheet,
                    keep_default_na=False,
                    na_values=na_values or list(self.DEFAULT_NA_VALUES),
                    dtype=str,  # Ensure all columns are read as strings
                    **kwargs,
                )
                for sheet in ([sheet_name] if sheet_name else excel_file.sheet_names)
            ]
        )

    @add_viadot_metadata_columns
    def to_df(
        self,
        url: str,
        sheet_name: str | list[str] | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        tests: dict[str, Any] | None = None,
        file_sheet_mapping: dict[str, str | int | list[str]] | None = None,
        na_values: list[str] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Load an Excel file or files from a SharePoint URL into a pandas DataFrame.

        This method handles downloading the file(s), parsing the content, and converting
        it into a pandas DataFrame. It supports both single file URLs and folder URLs
        with multiple files.

        Args:
            url (str): The URL of the file to be downloaded.
            sheet_name (Optional[Union[str, list, int]], optional): Strings are used for
                sheet names. Integers are used in zero-indexed sheet positions (chart
                sheets do not count as a sheet position). Lists of strings/integers are
                used to request multiple sheets. Specify None to get all worksheets.
                Defaults to None.
            if_empty (Literal["warn", "skip", "fail"], optional): Action to take if
                the DataFrame is empty.
                - "warn": Logs a warning.
                - "skip": Skips the operation.
                - "fail": Raises an error.
                Defaults to "warn".
            tests (Dict[str, Any], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from utils. Defaults to None.
            file_sheet_mapping (dict[str, Union[str, int, list[str]]], optional):
                Mapping of file names to sheet names or indices. The keys are file names
                and the values are sheet names/indices. Used when multiple files are
                involved. Defaults to None.
            na_values (list[str], optional): Additional strings to recognize as NA/NaN.
                If list passed, the specific NA values for each column will be
                recognized. Defaults to None.
            kwargs (dict[str, Any], optional): Keyword arguments to pass to
                pd.ExcelFile.parse(). Note that `nrows` is not supported.

        Returns:
            pd.DataFrame: The resulting data as a pandas DataFrame.

        Raises:
            ValueError: If the file extension is not supported or if `if_empty` is set
                to "fail" and the DataFrame is empty.
            SKIP: If `if_empty` is set to "skip" and the DataFrame is empty.
        """
        if self._is_file(url):
            df = self._load_and_parse(
                file_url=url, sheet_name=sheet_name, na_values=na_values, **kwargs
            )
        elif file_sheet_mapping:
            df = self._handle_multiple_files(
                url=url,
                file_sheet_mapping=file_sheet_mapping,
                na_values=na_values,
                **kwargs,
            )
        else:
            list_of_urls = self.scan_sharepoint_folder(url)
            excel_urls = [
                file_url
                for file_url in list_of_urls
                if self._get_file_extension(file_url) == ".xlsx"
            ]
            if not excel_urls:
                self.logger.warning(f"No Excel files found in folder: {url}")
                return pd.DataFrame()
            dfs = [
                self._load_and_parse(
                    file_url=file_url,
                    sheet_name=sheet_name,
                    na_values=na_values,
                    **kwargs,
                )
                for file_url in excel_urls
            ]
            df = pd.concat(validate_and_reorder_dfs_columns(dfs))

        if df.empty:
            try:
                self._handle_if_empty(if_empty)
            except SKIP:
                return pd.DataFrame()
        else:
            self.logger.info(f"Successfully downloaded {len(df)} rows of data.")

        df_clean = cleanup_df(df)

        if tests:
            validate(df=df_clean, tests=tests)

        return df_clean


class SharepointList(Sharepoint):
    """A class to connect to SharePoint lists and retrieve data."""

    def __init__(
        self,
        default_protocol: str | None = "https://",
        credentials: SharepointCredentials = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the SharepointList connector.

        Args:
            default_protocol (str, optional): The default protocol to use for
                SharePoint URLs.Defaults to "https://".
            credentials (SharepointCredentials, optional): SharePoint credentials.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials.
        """
        self.default_protocol = default_protocol
        super().__init__(
            *args, credentials=credentials, config_key=config_key, **kwargs
        )

    def _find_and_rename_case_insensitive_duplicated_column_names(
        self, df: pd.DataFrame
    ) -> dict:
        """Identifies case-insensitive duplicate column names in a DataFrame.

        This function is necessary because SharePoint lists can have columns
        with the same name but different cases (e.g., "ID" and "Id"),which can cause
        issues when processing the data. It renames these columns by appending a count
        suffix to ensure uniqueness.

        Columns are renamed based on appearance count. For example, if columns include
        ["ID", "Test", "Description", "Id"], the function will create rename mappings
        {"ID": "id_1", "Id": "id_2"} to ensure uniqueness.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            dict: A dictionary mapping duplicate column names to their new names.

        Raises:
            TypeError: If input is not a pandas DataFrame.

        Notes:
            This function iterates through the DataFrame's columns, tracking
            case-insensitive duplicates.
            Duplicate columns are renamed by appending a count
            suffix (e.g., "col_1", "col_2").
        """
        columns = df.columns.tolist()
        seen = {}
        rename_dict = {}

        for col in columns:
            col_lower = col.lower()
            if col_lower in seen:
                seen[col_lower] += 1
                rename_dict[col] = f"{col_lower}_{seen[col_lower]}"
            else:
                seen[col_lower] = 1
                # Check if this column needs to be renamed due to future duplicates
                duplicates = [c for c in columns if c.lower() == col_lower]
                if len(duplicates) > 1:
                    rename_dict[col] = f"{col_lower}_1"

        return rename_dict

    def _build_sharepoint_endpoint(
        self, site_url: str, list_site: str, list_name: str
    ) -> str:
        """Build the SharePoint REST API endpoint URL.

        Args:
            site_url: The base SharePoint site URL with protocol
            list_site: The specific site where the list is stored
            list_name: The name of the SharePoint list

        Returns:
            str: The constructed endpoint URL
        """
        return f"{site_url}/sites/{list_site}/_api/web/lists/GetByTitle('{list_name}')/items"

    def _ensure_protocol(self, site_url: str) -> str:
        """Ensure the site URL has the correct protocol.

        Args:
            site_url: The site URL to check

        Returns:
            str: The site URL with the default protocol if needed
        """
        if not site_url.lower().startswith(self.default_protocol.lower()):
            return f"{self.default_protocol}{site_url}"
        return site_url

    def _get_records(
        self, url: str, params: dict | None = None
    ) -> tuple[list[dict], str | None]:
        """Make a request to the SharePoint API and handle common errors.

        Args:
            url: The API endpoint URL
            params: Optional query parameters

        Returns:
            tuple: (data_items, next_link) where data_items is a list of items
                  and next_link is the URL for the next page or None
        """
        conn = self.get_connection()

        try:
            response = conn.get(url, params=params)
            response.raise_for_status()
        except TimeoutError as e:
            msg = f"Request to SharePoint list timed out: {e!s}"
            raise ValueError(msg) from e
        except requests.exceptions.HTTPError as e:
            status_code = getattr(
                getattr(e, "response", None), "status_code", "unknown"
            )
            msg = f"HTTP error {status_code} when retrieving data from SharePoint list"
            raise ValueError(msg) from e
        except Exception as e:
            msg = f"Failed to retrieve data from SharePoint list: {e!s}"
            raise ValueError(msg) from e

        data = response.json().get("d", {})
        items = data.get("results", [])

        # Get the URL for the next page
        next_link = data.get("__next")
        if isinstance(next_link, dict) and "uri" in next_link:
            next_link = next_link["uri"]

        return items, next_link

    def _paginate_list_data(
        self, initial_url: str, params: dict | None = None, list_name: str = ""
    ) -> list[dict]:
        """Handle pagination for SharePoint list data.

        Args:
            initial_url: The initial API endpoint URL
            params: Optional query parameters for the first request
            list_name: Name of the list for error messages

        Returns:
            list: All items from all pages
        """
        all_results = []
        next_url = initial_url
        first_request = True

        while next_url:
            # For first request, include original parameters
            # For subsequent requests, parameters are in the next_url
            current_params = params if first_request else None

            try:
                items, next_url = self._get_records(next_url, current_params)
                all_results.extend(items)
                first_request = False
            except ValueError as e:
                # Add the list_name to the error message
                msg = str(e).replace(
                    "SharePoint list", f"SharePoint list '{list_name}'"
                )
                raise ValueError(msg) from e

        if not all_results:
            msg = f"No items found in SharePoint list {list_name}"
            raise ValueError(msg)

        return all_results

    @add_viadot_metadata_columns
    def to_df(
        self,
        list_name: str,
        list_site: str,
        query: str | None = None,
        select: list[str] | None = None,
        tests: dict | None = None,
    ) -> pd.DataFrame:
        """Retrieve data from a SharePoint list as a pandas DataFrame.

        Args:
            list_site (str): The Sharepoint site on which the list is stored.
            list_name (str): The name of the SharePoint list.
            query (str, optional): A query to filter items. Defaults to None.
            select (list[str], optional): Fields to include in the response.
                Defaults to None.
            tests (Dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from utils. Defaults to None.

        Returns:
            pd.DataFrame: The list data as a DataFrame.

        Raises:
            ValueError: If the list does not exist or the request fails.
        """
        conn = self.get_connection()
        site_url = self._ensure_protocol(conn.site)
        endpoint = self._build_sharepoint_endpoint(site_url, list_site, list_name)

        # Build request parameters
        params = {}
        if query:
            params["$filter"] = query
        if select:
            params["$select"] = ",".join(select)

        # Get all items with pagination handling
        all_results = self._paginate_list_data(endpoint, params, list_name)

        # Convert to DataFrame
        df = pd.DataFrame(all_results)

        # Handle case-insensitive duplicate column names
        rename_dict = self._find_and_rename_case_insensitive_duplicated_column_names(df)
        df = df.rename(columns=rename_dict)

        return validate(df=df, tests=tests) if tests else df
