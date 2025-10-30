"""Sharepoint API connector."""

import io
from pathlib import Path
import re
from typing import Any, Literal
from urllib.parse import urlparse

import msal
from office365.graph_client import GraphClient
import pandas as pd
from pandas._libs.parsers import STR_NA_VALUES
from pydantic import BaseModel, root_validator

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
    client_id: str  # Sharepoint client id
    client_secret: str  # Sharepoint client secret
    tenant_id: str  # Sharepoint tenant id

    @root_validator(pre=True)
    def is_configured(cls, credentials: dict):  # noqa: N805, ANN201, D102
        site = credentials.get("site")
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        tenant_id = credentials.get("tenant_id")

        if not (site and client_id and client_secret and tenant_id):
            msg = "'site', 'client_id', 'client_secret' and 'tenant_id' credentials are required."
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

    def get_client(self) -> GraphClient:
        """Establishe a connection to SharePoint.

        Returns:
            GraphClient: A session object representing
                the authenticated connection.

        Raises:
            CredentialError: If authentication to SharePoint fails due to incorrect
                credentials.
        """
        try:
            client = GraphClient(self._acquire_token_func)
        except Exception as e:
            site = self.credentials.get("site")
            msg = f"Could not authenticate to {site} with provided credentials."
            raise CredentialError(msg) from e
        return client

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
        client = self.get_client()
        file_item = client.shares.by_url(url).drive_item.get().execute_query()
        with Path(to_path).open("wb") as local_file:
            file_item.download(local_file).execute_query()

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
        client = self.get_client()

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

        folder_item = client.shares.by_url(url).drive_item.get().execute_query()
        files = folder_item.children.get().execute_query()

        return [f"{site_url}/{library}{file.name}" for file in files]

    def _acquire_token_func(self):
        """Acquire token via MSAL.

        Returns:
            str: A string containing the access token.
        """
        authority_url = (
            f'https://login.microsoftonline.com/{self.credentials.get("tenant_id")}'
        )

        app = msal.ConfidentialClientApplication(
            authority=authority_url,
            client_id=self.credentials.get("client_id"),
            client_credential=self.credentials.get("client_secret"),
        )

        return app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )

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

        Raises:
            ValueError: If the parameter 'nrows' is not supported.
        """
        if "nrows" in kwargs:
            msg = "Parameter 'nrows' is not supported."
            raise ValueError(msg)

        client = self.get_client()

        self.logger.info(f"Downloading data from {url} ...")
        try:
            bytes_buffer = io.BytesIO()
            file_item = client.shares.by_url(url).drive_item.get().execute_query()
            file_item.download(bytes_buffer).execute_query()
            bytes_buffer.seek(0)
        except Exception:
            self.logger.exception(f"Failed to download file: {url}")
            raise

        try:
            return pd.ExcelFile(bytes_buffer.getvalue())
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
            if sheet_name:
                df["sheet_name"] = sheet_name
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

    def _parse_rest_list_url(self, url: str) -> tuple[str, str, str]:
        """Parse a REST list URL and return (host, site_path, list_name).

        Args:
            url: The URL to parse

        Returns:
            tuple: A tuple containing the host, site path, and list name
        """
        parsed = urlparse(url)
        path = parsed.path
        host = parsed.netloc

        site_match = re.search(r"/sites/([^/]+)/", path)
        list_match = re.search(r"GetByTitle\('([^']+)'\)", path)
        if not site_match or not list_match:
            msg = "Provided URL format is not supported for Graph-based retrieval."
            raise ValueError(msg)

        site_segment = site_match.group(1)
        site_path = f"/sites/{site_segment}"
        list_name = list_match.group(1)
        return host, site_path, list_name

    def _fetch_collection(
        self,
        client: GraphClient,
        host: str,
        site_path: str,
        list_name: str,
        params: dict | None,
    ) -> tuple[object, list[str]]:
        """Resolve site and list, apply params.

        Args:
            client (GraphClient): The GraphClient object.
            host (str): The host of the SharePoint site.
            site_path (str): The path of the SharePoint site.
            list_name (str): The name of the SharePoint list.
            params (dict): The parameters to apply to the collection.

        Returns:
            collection (object): The collection of list items.
            selected_fields (list[str]): The selected fields.
        """
        site_url = f"https://{host}{site_path}"
        site = client.sites.get_by_url(site_url).get().execute_query()
        lists = site.lists.get().execute_query()
        target_list = next(
            (lst for lst in lists if getattr(lst, "display_name", None) == list_name),
            None,
        )
        if target_list is None:
            msg = f"Failed to retrieve data from SharePoint list '{list_name}': list not found"
            raise ValueError(msg)

        collection = target_list.items
        if params and params.get("$filter"):
            collection = collection.filter(
                params["$filter"]
            )  # Graph expects fields/FieldName in filter
        collection = collection.expand(["fields"]).get().execute_query()

        selected_fields = None
        if params and params.get("$select"):
            selected_fields = [
                f.strip() for f in str(params["$select"]).split(",") if f.strip()
            ]
        return collection, selected_fields

    def _serialize_via_methods(self, value: object) -> object | None:
        """Serialize a value via methods.

        Args:
            value (object): The value to serialize

        Returns:
            object: The serialized value
        """
        for attr in ("to_json", "serialize", "to_dict"):
            method = getattr(value, attr, None)
            if callable(method):
                try:
                    return method()
                except Exception:
                    # Log at debug level to avoid noisy logs while satisfying linter
                    self.logger.debug(
                        f"{attr}() serialization failed for {type(value).__name__}",
                        exc_info=True,
                    )
                    continue
        return None

    def _try_isoformat(self, value: object) -> str | None:
        """Try to format a value as ISO format.

        Args:
            value (object): The value to format

        Returns:
            str: The formatted value
        """
        isoformat = getattr(value, "isoformat", None)
        if callable(isoformat):
            try:
                return value.isoformat()
            except Exception:
                return None
        return None

    def _introspect_properties(self, value: object) -> dict[str, object] | None:
        """Introspect the properties of a value.

        Args:
            value (object): The value to introspect

        Returns:
            dict[str, object]: The properties of the value
        """
        props = getattr(value, "properties", None)
        if isinstance(props, dict):
            return {k: self._to_plain(v) for k, v in props.items()}
        return None

    def _introspect_dunder(self, value: object) -> dict[str, object] | None:
        """Introspect the __dict__ of a value.

        Args:
            value (object): The value to introspect

        Returns:
            dict[str, object]: The __dict__ of the value
        """
        dunder_dict = getattr(value, "__dict__", None)
        if isinstance(dunder_dict, dict):
            cleaned = {
                k: self._to_plain(v)
                for k, v in dunder_dict.items()
                if not k.startswith("_") and not callable(v)
            }
            if cleaned:
                return cleaned
        return None

    def _to_plain(self, value: object) -> object:
        """Recursively convert Office365 SDK objects to JSON-serializable values.

        Args:
            value (object): The value to convert

        Returns:
            object: The converted value
        """
        result: object
        # Primitives
        if value is None or isinstance(value, str | int | float | bool):
            result = value
        else:
            # Date-like objects
            dt = self._try_isoformat(value)
            if dt is not None:
                result = dt
            elif isinstance(value, dict):
                result = {k: self._to_plain(v) for k, v in value.items()}
            elif isinstance(value, list | tuple | set):
                result = [self._to_plain(v) for v in list(value)]
            else:
                # Office365 SDK / client objects via serializer methods
                serialized = self._serialize_via_methods(value)
                if serialized is not None:
                    result = self._to_plain(serialized)
                else:
                    # Fallbacks: properties and __dict__
                    props = self._introspect_properties(value)
                    if props is not None:
                        result = props
                    else:
                        dunder = self._introspect_dunder(value)
                        result = dunder if dunder is not None else str(value)
        return result

    def _flatten_dict(
        self, data: dict[str, object], parent_key: str = "", sep: str = "_"
    ) -> dict[str, object]:
        """Flatten a nested dictionary using separator and lowercase keys.

        Args:
            data (dict[str, object]): The dictionary to flatten
            parent_key (str): The parent key
            sep (str): The separator

        Returns:
            dict[str, object]: The flattened dictionary
        """
        items: dict[str, object] = {}
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
            if isinstance(value, dict):
                items.update(self._flatten_dict(value, new_key, sep=sep))
            else:
                items[new_key] = value
        return items

    def _flatten_record(self, record: dict[str, object]) -> dict[str, object]:
        """Flatten nested dict values at top-level keys of a record.

        - Removes redundant 'fields' key if present (its contents are already merged).
        - Flattens any dict-valued fields into "key_subkey" form.

        Args:
            record (dict[str, object]): The record to flatten

        Returns:
            dict[str, object]: The flattened record
        """
        record = dict(record)
        # Remove redundant 'fields' duplicate container if present
        record.pop("fields", None)

        flattened: dict[str, object] = {}
        for key, value in record.items():
            if isinstance(value, dict):
                nested = self._flatten_dict(value, key)
                flattened.update(nested)
            else:
                flattened[key] = value
        return flattened

    def _collect_item_dicts(
        self, collection: object, selected_fields: list[str] | None
    ) -> list[dict]:
        """Convert a collection of list items into a list of dicts.

        Args:
            collection (object): The collection of list items.
            selected_fields (list[str]): The fields to select.

        Returns:
            list[dict]: A list of dicts containing the collection of list items.
        """
        results: list[dict] = []
        for item in collection:
            fields_obj = getattr(item, "fields", None)
            fields_props = (
                fields_obj.properties
                if fields_obj is not None and hasattr(fields_obj, "properties")
                else {}
            )
            item_props = item.properties if hasattr(item, "properties") else {}
            combined = {**fields_props, **item_props}
            if selected_fields:
                combined = {k: v for k, v in combined.items() if k in selected_fields}
            # Normalize complex SDK objects to plain values
            normalized = {k: self._to_plain(v) for k, v in combined.items()}
            # Drop redundant nested containers and flatten nested dicts
            flattened = self._flatten_record(normalized)
            # Ensure lowercase column names
            lowered = {str(k).lower(): v for k, v in flattened.items()}
            results.append(lowered)
        return results

    def _extract_next_link(self, collection: object) -> str | None:
        """Extract next page URL from a Graph SDK collection if available.

        Args:
            collection (object): The collection of list items.

        Returns:
            str | None: The next page URL or None.
        """
        next_page_request = getattr(collection, "next_page_request", None)
        if next_page_request is not None:
            next_link = getattr(next_page_request, "url", None) or getattr(
                next_page_request, "request_url", None
            )
            if next_link is not None:
                return next_link
            build_req = getattr(next_page_request, "build_request", None)
            if callable(build_req):
                try:
                    req_obj = build_req()
                    return getattr(req_obj, "url", None) or getattr(
                        req_obj, "request_url", None
                    )
                except Exception:
                    return None
        return None

    def _get_records(
        self, url: str, params: dict | None = None
    ) -> tuple[list[dict], str | None]:
        """Make a request to the SharePoint API and handle common errors.

        Args:
            url (str): The API endpoint URL
            params (dict): Optional query parameters

        Returns:
            tuple: (data_items, next_link) where data_items is a list of items
                  and next_link is the URL for the next page or None
        """
        try:
            host, site_path, list_name = self._parse_rest_list_url(url)
            client = self.get_client()
            collection, selected_fields = self._fetch_collection(
                client, host, site_path, list_name, params
            )
            results = self._collect_item_dicts(collection, selected_fields)
            next_link = self._extract_next_link(collection)
            return results, next_link
        except TimeoutError as e:
            msg = f"Request to SharePoint list timed out: {e!s}"
            raise ValueError(msg) from e
        except Exception as e:
            msg = f"Failed to retrieve data from SharePoint list: {e!s}"
            raise ValueError(msg) from e

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
        site = self.credentials.get("site")
        site_url = self._ensure_protocol(site)
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

        if tests:
            validate(df=df, tests=tests)

        return df
