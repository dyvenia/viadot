"""SMB source connector for Viadot using pure Python libraries.

This module provides functionality to connect to SMB (Server Message Block) shares,
list directories, download files, and stream files to S3. It uses the `smbclient`
library for SMB operations and `boto3` for S3 interactions, avoiding external CLI tools.

The module includes:
- Helper functions for SMB path manipulation and file operations
- SMBClient class for high-level SMB operations
- Support for organizing files by year extracted from paths
"""

from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
import re
import threading
from typing import Any
from urllib.parse import urlparse
import zipfile

import boto3
from botocore.exceptions import ClientError
import pendulum
import smbclient
import smbprotocol

from viadot.config import get_source_credentials
from viadot.exceptions import (
    CredentialError,
    SMBConnectionError,
    SMBFileOperationError,
    SMBInvalidFilenameError,
)
from viadot.orchestration.prefect.utils import DynamicDateHandler
from viadot.sources.base import Source
from viadot.sources.smb import SMBCredentials


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _ensure_smb_session(server: str, username: str, password: str) -> None:
    """Register (or reuse) an SMB session for a given server.

    `python-smbclient` keeps a global session cache, so repeated calls are cheap.
    This function ensures a session is established before any SMB operations.

    Args:
        server (str): SMB server hostname or IP address.
        username (str): SMB username for authentication.
        password (str): SMB password for authentication.

    Raises:
        SMBConnectionError: If authentication fails or connection cannot be established.
    """
    if not (server and username and password):
        msg = "SMB server/username/password must be provided."
        raise SMBConnectionError(msg)

    try:
        smbclient.register_session(server, username=username, password=password)
    except smbprotocol.exceptions.LogonFailure as e:
        msg = "Authentication failed: invalid SMB credentials."
        raise SMBConnectionError(msg) from e
    except smbprotocol.exceptions.PasswordExpired as e:
        msg = "Authentication failed: SMB password expired."
        raise SMBConnectionError(msg) from e
    except Exception as e:
        msg = f"SMB connection failed: {e}"
        raise SMBConnectionError(msg) from e


def _to_unc_path(server: str, share: str, relative_path: str | None) -> str:
    r"""Build UNC path for python `smbclient` from server/share and a relative path.

    Args:
        server (str): SMB server hostname or IP address.
        share (str): SMB share name.
        relative_path (str | None): Relative path within the share.

    Returns:
        str: UNC path like `\\\\server\\share\\relative\\path`.

    Examples:
        >>> _to_unc_path("myserver", "myshare", "data/file.txt")
        '\\\\myserver\\myshare\\data\\file.txt'
        >>> _to_unc_path("myserver", "myshare", None)
        '\\\\myserver\\myshare'
    """
    rel = (relative_path or "").strip("/").strip("\\")
    if rel:
        rel = rel.replace("/", "\\")
        return f"\\\\{server}\\{share}\\{rel}"
    return f"\\\\{server}\\{share}"


def _to_rel_path(base_rel: str, name: str) -> str:
    """Build relative path by combining base path with filename.

    Args:
        base_rel (str): Base relative path.
        name (str): Filename or subdirectory name to append.

    Returns:
        str: Combined relative path with forward slashes.

    Examples:
        >>> _to_rel_path("data", "file.txt")
        'data/file.txt'
        >>> _to_rel_path("", "file.txt")
        'file.txt'
    """
    base = (base_rel or "").strip("/").strip("\\")
    if not base:
        return name
    return f"{base}/{name}"


def _check_filename_for_problematic_chars(filename: str) -> bool:
    """Check if filename contains characters that may cause issues with smbclient.

    Args:
        filename (str): Filename to check

    Returns:
        True if filename contains problematic characters, False otherwise
    """
    problematic_chars = [";", "|", "`", "$"]

    return any(char in filename for char in problematic_chars)


def _get_unique_local_path(local_dir: str, filename: str) -> str:
    """Generate a unique local file path by adding _n suffix if file already exists.

    Args:
        local_dir (str): Local directory where file will be saved
        filename (str): Original filename

    Returns:
        Unique local file path with _n suffix if necessary

    Example:
        If 'file.xlsx' exists, returns path with 'file_1.xlsx'
        If 'file_1.xlsx' also exists, returns path with 'file_2.xlsx'
    """
    # Split filename into name and extension
    file_path = Path(filename)
    name = file_path.stem  # filename without extension
    extension = file_path.suffix  # .xlsx, .xls, etc.

    # Try original filename first
    full_path = Path(local_dir) / filename
    if not full_path.exists():
        return str(full_path)

    # File exists, try adding _n suffix
    counter = 1
    while True:
        new_filename = f"{name}_{counter}{extension}"
        full_path = Path(local_dir) / new_filename
        if not full_path.exists():
            logger.info(
                f"File '{filename}' already exists, using '{new_filename}' instead"
            )
            return str(full_path)
        counter += 1


def _build_prefix_from_path(file_path: str, levels: int) -> str:
    """Extract parent folder names to use as prefix.

    Args:
        file_path (str): The full or relative remote path to the file on SMB share.
        levels (int): Number of parent folder levels to include.

    Returns:
        Prefix string with parent folder names joined by underscores.
        Returns empty string if levels is 0 or no valid parents exist.
    """
    if levels <= 0:
        return ""

    # Normalize path separators to POSIX style
    normalized_path = file_path.replace("\\", "/")
    path = Path(normalized_path)

    # Filter out invalid/empty path components
    parent_parts = [
        p for p in path.parent.parts if p and p not in ("/", "\\", ".", "//", "")
    ]

    # Clamp levels to valid range
    valid_levels = min(levels, len(parent_parts))

    if valid_levels == 0:
        return ""

    # Take the last N parent folders (closest to file)
    prefix_parts = parent_parts[-valid_levels:]

    return "_".join(prefix_parts)


def _add_prefix_to_filename(filename: str, prefix: str) -> str:
    """Combine prefix and filename with underscore separator.

    Args:
        filename (str): The base filename.
        prefix (str): The prefix to add.

    Returns:
        Prefixed filename, or original filename if prefix is empty.
    """
    if not prefix:
        return filename
    return f"{prefix}_{filename}"


def extract_year_from_path(file_path: str) -> str | None:
    """Extract year (4-digit, 1900-2099) from file path if present.

    Searches for 4-digit numbers between 1900-2099 in the file path. If multiple
    years are found, returns the last one (closest to the file), as it's most
    likely the relevant year for the file.

    Args:
        file_path (str): The file path to search for years.

    Returns:
        str | None: The extracted year as a string, or None if no valid year found.

    Examples:
        >>> extract_year_from_path("2024/data/file.xlsx")
        '2024'
        >>> extract_year_from_path("2023/reports/2024/file.xlsx")
        '2024'
        >>> extract_year_from_path("data/file.xlsx")
        None
    """
    # Match 4-digit year (1900-2099) in the path
    year_pattern = r"\b(19\d{2}|20[0-9]{2})\b"
    matches = re.findall(year_pattern, file_path)
    if matches:
        # Return the last year found (closest to the file in the path)
        return matches[-1]
    return None


class SMBItemType(Enum):
    """Enumeration of SMB item types."""

    DIRECTORY = "D"
    FILE = "A"
    # Add other types as needed based on smbclient output


class SMBItem:
    """Represents a file or directory item from an SMB share.

    This class provides a structured way to represent SMB directory listings
    with support for nested directory structures and file filtering.
    """

    def __init__(
        self,
        name: str,
        item_type: SMBItemType,
        path: str = "",
        date_modified: datetime | None = None,
        size: int | None = None,
        children: list["SMBItem"] | None = None,
        needs_expansion: bool = False,
    ):
        """Initialize SMB item with name, type, path, date, size, and children.

        Args:
            name (str): Item name.
            item_type (SMBItemType): FILE or DIRECTORY.
            path (str): Full path to this item.
            date_modified (datetime | None): Last modification date.
            size (int | None): File size in bytes.
            children (list[SMBItem] | None): Child items for directories.
            needs_expansion (bool): True if directory needs recursive expansion later.
        """
        self.name = name
        self.type = item_type
        self.path = path  # Full path to this item
        self.date_modified = date_modified
        self.size = size  # File size in bytes
        self.children = children or []
        self.needs_expansion = needs_expansion  # For fallback shallow listing

    def get_path(self) -> str:
        """Return the full path to this file/folder."""
        return self.path

    def get_matching_file_paths_in_range(
        self,
        filename_regex: str | list[str] | None = None,
        filepath_regex: str | list[str] | None = None,
        extensions: str | list[str] | None = None,
        date_filter: str | tuple[str, str] | None = None,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y-%m-%d",
        dynamic_date_timezone: str = "UTC",
        exclude_directories: bool = False,
        full_path_prefix: str | None = None,
    ) -> list[str]:
        """Get paths of items that match the specified filtering criteria.

        Uses the internal _is_matching_file method to check if items meet the criteria.

        Args:
            filename_regex (str | list[str] | None): Regular expression pattern(s) to
                filter file names (only the basename, without the directory path). If
                provided **and `filepath_regex` is not set**, only file names matching
                the pattern(s) will be included. Defaults to None.
            filepath_regex (str | list[str] | None): Regular expression pattern(s) to
                filter the **full path** (including directories and file name). If
                provided, it takes precedence over `filename_regex` and only paths
                matching the pattern(s) will be included. Defaults to None.
            extensions (str | list[str] | None): File extensions to filter by.
                Case-insensitive. Defaults to None.
            date_filter (str | tuple[str, str] | None): Date filter to apply. Can be a
                single date string or tuple of (start, end) date strings. Supports
                dynamic date symbols. Defaults to None.
            dynamic_date_symbols (list[str]): Symbols for dynamic date handling.
                Defaults to ["<<", ">>"].
            dynamic_date_format (str): Format used for dynamic date parsing.
                Defaults to "%Y-%m-%d".
            dynamic_date_timezone (str): Timezone used for dynamic date processing.
                Defaults to "UTC".
            exclude_directories (bool): If True, only return file paths (exclude
                directories). Defaults to False.
            full_path_prefix (str | None): If provided, prepend this prefix to all
                returned paths to create full SMB paths (e.g., "//server/share/").
                Defaults to None.

        Returns:
            list[str]: List of paths that match the filtering criteria.
        """
        paths = []

        # Convert datetime to pendulum.Date for _is_matching_file if date exists
        file_mod_date_parsed = None
        if self.date_modified:
            # Convert datetime to pendulum.Date
            file_mod_date_parsed = pendulum.instance(self.date_modified).date()

        # Check if this item should be included based on exclude_directories and filters
        should_include = True

        # If exclude_directories is True, only include files (not directories)
        if exclude_directories and self.type == SMBItemType.DIRECTORY:
            should_include = False
        else:
            # Check if this item matches the filtering criteria using _is_matching_file
            should_include = self._is_matching_file(
                file_name=self.name,
                file_path=self.path,
                file_mod_date_parsed=file_mod_date_parsed,
                filename_regex=filename_regex,
                filepath_regex=filepath_regex,
                extensions=extensions,
                date_filter=date_filter,
                dynamic_date_symbols=dynamic_date_symbols,
                dynamic_date_format=dynamic_date_format,
                dynamic_date_timezone=dynamic_date_timezone,
            )

        if should_include:
            paths.append(self.path)

        # Recursively check children
        for child in self.children:
            paths.extend(
                child.get_matching_file_paths_in_range(
                    filename_regex=filename_regex,
                    filepath_regex=filepath_regex,
                    extensions=extensions,
                    date_filter=date_filter,
                    dynamic_date_symbols=dynamic_date_symbols,
                    dynamic_date_format=dynamic_date_format,
                    dynamic_date_timezone=dynamic_date_timezone,
                    exclude_directories=exclude_directories,
                    full_path_prefix=None,  # Don't pass prefix to children
                )
            )

        # Add full path prefix if provided (only once at the top level)
        if full_path_prefix:
            separator = "/"
            paths = [
                f"{full_path_prefix.rstrip(separator).rstrip('/')}{separator}{path.lstrip(separator).lstrip('/')}"
                for path in paths
            ]

        return paths

    def to_dict(self) -> dict[str, Any]:
        """Convert the SMB item to a dictionary representation.

        Returns:
            dict[str, Any]: Dictionary containing the SMB item's data including name,
                type, path, children (recursively converted), and optionally
                date_modified and size if they are set.
        """
        result = {
            "name": self.name,
            "type": self.type.value,
            "path": self.path,
            "children": [child.to_dict() for child in self.children],
        }
        if self.date_modified:
            result["date_modified"] = self.date_modified.isoformat()
        if self.size is not None:
            result["size"] = self.size
        return result

    def __repr__(self):
        """String representation of the SMB item."""
        date_str = f", date_modified={self.date_modified}" if self.date_modified else ""
        size_str = f", size={self.size}" if self.size is not None else ""
        return f"SMBItem(name='{self.name}', type={self.type}, path='{self.path}'{date_str}{size_str}, children={len(self.children)} items)"

    def _parse_dates(
        self,
        date_filter: str | tuple[str, str] | None = None,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y-%m-%d",
        dynamic_date_timezone: str = "UTC",
    ) -> pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None:
        """Parses a date or date range, supporting dynamic date symbols.

        Args:
            date_filter (str | tuple[str, str] | None):
                - A single date string (e.g., "2024-03-03").
                - A tuple containing exactly two date strings, 'start' and 'end' date.
                - None, which applies no date filter.
                Defaults to None.
            dynamic_date_symbols (list[str]): Symbols for dynamic date handling.
                Defaults to ["<<", ">>"].
            dynamic_date_format (str): Format used for dynamic date parsing.
                Defaults to "%Y-%m-%d".
            dynamic_date_timezone (str): Timezone used for dynamic date processing.
                Defaults to "UTC".

        Returns:
            pendulum.Date: If a single date is provided.
            tuple[pendulum.Date, pendulum.Date]: If a date range is provided.
            None: If `date_filter` is None.

        Raises:
            ValueError: If `date_filter` is neither a string nor a tuple of exactly
                two strings.
        """
        if date_filter is None:
            return None

        ddh = DynamicDateHandler(
            dynamic_date_symbols=dynamic_date_symbols,
            dynamic_date_format=dynamic_date_format,
            dynamic_date_timezone=dynamic_date_timezone,
        )

        match date_filter:
            case str():
                return pendulum.parse(ddh.process_dates(date_filter)).date()

            case (start, end) if isinstance(start, str) and isinstance(end, str):
                return (
                    pendulum.parse(ddh.process_dates(start)).date(),
                    pendulum.parse(ddh.process_dates(end)).date(),
                )

            case _:
                msg = (
                    "date_filter must be a string, a tuple of exactly 2 dates, or None."
                )
                raise ValueError(msg)

    def _safe_regex_match(self, pattern: str, text: str) -> bool:
        """Evaluate whether a regex pattern matches given text (case-insensitive).

        This method wraps `re.search` with error handling to catch and log invalid regex
        patterns without interrupting execution.

        Args:
            pattern (str): The regular expression pattern to match against.
            text (str): The input string to search within.

        Returns:
            bool: True if the pattern matches the text; False if it does not match
                or if the pattern is invalid.
        """
        try:
            return re.search(pattern, text, re.IGNORECASE) is not None
        except re.error as e:
            # Use module-level logger since SMBItem does not define an instance logger
            logger.warning(f"Invalid regex pattern: {pattern} — Error: {e}")
            return False

    def _matches_any_regex(
        self,
        text: str,
        patterns: str | list[str] | None = None,
    ) -> bool:
        """Check whether the given text matches any of the provided regex pattern(s).

        Args:
            text (str): The string to test.
            patterns (str | list[str] | None): A regex pattern or list of patterns.
                If None, this method returns True (no filtering applied).

        Returns:
            bool: True if the text matches at least one pattern, or if no pattern
                is given.
        """
        if not patterns:
            return True

        if isinstance(patterns, str):
            patterns = [patterns]

        return any(self._safe_regex_match(pattern, text) for pattern in patterns)

    def _is_date_match(
        self,
        file_modification_date: pendulum.Date | None,
        date_filter_parsed: pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None,
    ) -> bool:
        """Check if the file modification date matches the given date filter.

        Args:
            file_modification_date (pendulum.Date | None): The modification date of the
                file to check.
            date_filter_parsed (pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None):
                The parsed date filter - either a single date, date range tuple, or None.
                If None, the file will not match any date filter.

        Returns:
            bool: True if the file_modification_date matches the filter or if no filter
                is applied. False otherwise.
        """  # noqa: W505
        if date_filter_parsed is None:
            return True

        if file_modification_date is None:
            return False

        if isinstance(date_filter_parsed, pendulum.Date):
            return file_modification_date == date_filter_parsed

        if isinstance(date_filter_parsed, tuple):
            start_date, end_date = date_filter_parsed
            return start_date <= file_modification_date <= end_date

        return False

    def _is_matching_file(
        self,
        file_name: str,
        file_path: str,
        file_mod_date_parsed: pendulum.Date | None,
        filename_regex: str | list[str] | None = None,
        filepath_regex: str | list[str] | None = None,
        extensions: str | list[str] | None = None,
        date_filter: str | tuple[str, str] | None = None,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y-%m-%d",
        dynamic_date_timezone: str = "UTC",
    ) -> bool:
        """Check if a file matches the given criteria.

        It verifies whether the file satisfies any combination of:
        - Filename regular expression filtering (basename only).
        - Full-path regular expression filtering (directory path + filename).
        - Extension-based filtering.
        - Exact date or date range filtering.

        Args:
            file_name (str): The **basename** of the file to evaluate (without path).
            file_path (str): The full path of the file, relative to the SMB root.
            file_mod_date_parsed (pendulum.Date | None): The parsed modification date of
                the file. If None, date filtering will not be applied.
            filename_regex (str | list[str] | None, optional): A regular expression
                string or list of regex patterns used to filter file names
                (basename only). Used **only if `filepath_regex` is not provided**.
                Defaults to None.
            filepath_regex (str | list[str] | None, optional): A regular expression
                string or list of regex patterns used to filter the full path
                (`file_path`). If provided, it takes precedence over
                `filename_regex`. Defaults to None.
            extensions (str | list[str] | None): List of file extensions or single
                string to filter by. It is case-insensitive. Defaults to None.
            date_filter (str | tuple[str, str] | None): Date filter to apply.
                Can be a single date string or tuple of (start, end) date strings.
                Supports dynamic date symbols. Defaults to None.
            dynamic_date_symbols (list[str]): Symbols for dynamic date handling.
                Defaults to ["<<", ">>"].
            dynamic_date_format (str): Format used for dynamic date parsing.
                Defaults to "%Y-%m-%d".
            dynamic_date_timezone (str): Timezone used for dynamic date processing.
                Defaults to "UTC".

        Returns:
            bool: True if the file matches all criteria or no criteria are provided,
                False otherwise.
        """
        name_lower = file_name.lower()
        path_lower = file_path.lower()

        # Normalize to lists
        filename_regex_list = (
            [filename_regex] if isinstance(filename_regex, str) else filename_regex
        )
        filepath_regex_list = (
            [filepath_regex] if isinstance(filepath_regex, str) else filepath_regex
        )
        extension_list = [extensions] if isinstance(extensions, str) else extensions

        matches_extension = not extension_list or any(
            isinstance(ext, str) and name_lower.endswith(ext.lower())
            for ext in extension_list
        )

        # - If filepath_regex is provided -> use it on the full path.
        # - Else if filename_regex is provided -> use it on the basename.
        # - If neither is provided -> treat as match (no name/path filter).
        if filepath_regex_list:
            matches_name_or_path = any(
                self._safe_regex_match(pattern, path_lower)
                for pattern in filepath_regex_list
            )
        elif filename_regex_list:
            matches_name_or_path = any(
                self._safe_regex_match(pattern, name_lower)
                for pattern in filename_regex_list
            )
        else:
            matches_name_or_path = True

        if not matches_extension or not matches_name_or_path:
            return False

        # Parse date filter and check date match
        date_filter_parsed = self._parse_dates(
            date_filter=date_filter,
            dynamic_date_symbols=dynamic_date_symbols,
            dynamic_date_format=dynamic_date_format,
            dynamic_date_timezone=dynamic_date_timezone,
        )

        if date_filter_parsed:
            return self._is_date_match(file_mod_date_parsed, date_filter_parsed)

        return True


def download_file_from_smb(  # noqa: C901, PLR0915
    server: str,
    share: str,
    remote_path: str,
    username: str,
    password: str,
    local_path: str = "/tmp",  # noqa: S108
    timeout: int = 900,
    prefix_levels_to_add: int = 0,
    zip_inner_file_regexes: str | list[str] | None = None,
) -> list[str]:
    """Download a file from SMB server to local filesystem.

    For ZIP files, if `zip_inner_file_regexes` is provided, extracts and saves
    only files matching the regex patterns.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        remote_path (str): Path to the file on SMB server (relative to share root).
        username (str): SMB username.
        password (str): SMB password.
        local_path (str): Local directory to save the file. Defaults to "/tmp".
        timeout (int): Timeout in seconds for the SMB operation. Defaults to 900.
        prefix_levels_to_add (int): Number of parent folder levels (from the remote
            path) to prepend as an underscore-separated prefix to the local filename.
            Example: for remote path "2025/02/file.xlsx" and levels=2, local file name
            will be "2025_02_file.xlsx". Defaults to 0 (no prefix).
        zip_inner_file_regexes (str | list[str] | None): Regular expression string
            or list of regex patterns used to filter files *inside* ZIP archives.
            If provided and the file is a ZIP, only matching inner files will be
            extracted and saved locally. Defaults to None.

    Returns:
        list[str]: A list of local paths to downloaded/extracted files.
            For regular files, returns a list with one path. For ZIP files with
            filtering, returns a list of paths to all extracted files that matched
            the regex patterns.

    Raises:
        SMBFileOperationError: If the download fails.
        SMBInvalidFilenameError: If filename contains problematic characters.
    """
    logger.info(f"Downloading file from SMB: {remote_path} from {server}/{share}")

    _ensure_smb_session(server=server, username=username, password=password)

    # Extract filename from remote path (basename)
    filename = remote_path.split("\\")[-1].split("/")[-1]

    # Optionally build prefix from parent directories of the remote path
    if prefix_levels_to_add and prefix_levels_to_add > 0:
        prefix = _build_prefix_from_path(remote_path, prefix_levels_to_add)
        filename = _add_prefix_to_filename(filename, prefix)

    # Check for problematic characters in filename
    if _check_filename_for_problematic_chars(filename):
        error_msg = (
            f"Skipping file with problematic characters in name: {filename} "
            f"(path: {remote_path})"
        )
        logger.warning(error_msg)
        raise SMBInvalidFilenameError(error_msg)

    Path(local_path).mkdir(parents=True, exist_ok=True)
    full_local_path = _get_unique_local_path(local_path.rstrip("/"), filename)

    unc_remote_path = _to_unc_path(
        server=server, share=share, relative_path=remote_path
    )

    result_container: dict[str, list[str]] = {}
    exception_container: dict[str, Exception] = {}

    def run_download() -> None:  # noqa: C901
        """Run the download operation in a separate thread with error handling."""
        try:
            # Check if this is a ZIP file with inner file filtering
            is_zip = filename.lower().endswith(".zip") and zip_inner_file_regexes

            if is_zip:
                # Handle ZIP file extraction with filtering
                extracted_paths = []
                with (
                    smbclient.open_file(unc_remote_path, mode="rb") as remote_file,
                    zipfile.ZipFile(remote_file) as zf,
                ):
                    for zip_member_name in zf.namelist():
                        # Skip directories inside ZIP
                        if zip_member_name.endswith("/"):
                            continue

                        # Check if file matches any of the regex patterns
                        if _matches_any_regex_module(
                            zip_member_name, zip_inner_file_regexes
                        ):
                            # Extract the file
                            base_name = Path(zip_member_name).name
                            local_file_path = _get_unique_local_path(
                                local_path.rstrip("/"), base_name
                            )

                            with (
                                zf.open(zip_member_name) as member,
                                Path(local_file_path).open("wb") as local_file,
                            ):
                                while True:
                                    chunk = member.read(1024 * 1024)
                                    if not chunk:
                                        break
                                    local_file.write(chunk)

                            extracted_paths.append(local_file_path)
                            logger.info(f"Extracted from ZIP: {local_file_path}")

                    if not extracted_paths:
                        logger.warning(f"No matching files found in ZIP: {remote_path}")

                result_container["paths"] = extracted_paths
            else:
                # Handle regular file download
                with (
                    smbclient.open_file(unc_remote_path, mode="rb") as remote_file,
                    Path(full_local_path).open("wb") as local_file,
                ):
                    while True:
                        chunk = remote_file.read(1024 * 1024)
                        if not chunk:
                            break
                        local_file.write(chunk)
                result_container["paths"] = [full_local_path]
        except Exception as e:
            exception_container["error"] = e

    download_thread = threading.Thread(target=run_download, daemon=True)
    download_thread.start()
    download_thread.join(timeout=timeout)

    if download_thread.is_alive():
        error_msg = f"SMB download timed out after {timeout} seconds"
        logger.error(error_msg)
        raise SMBFileOperationError(error_msg)

    if "error" in exception_container:
        e = exception_container["error"]
        error_msg = (
            f"SMB download failed. Server: {server}, Share: {share}, Path: {remote_path}. "
            f"Error: {e}"
        )
        logger.exception(error_msg)
        raise SMBFileOperationError(error_msg) from e

    paths = result_container["paths"]
    if len(paths) == 1:
        logger.info(f"Successfully downloaded file to: {paths[0]}")
    else:
        logger.info(
            f"Successfully extracted {len(paths)} files from ZIP: {remote_path}"
        )
    return paths


def _stream_file_from_smb_to_s3(  # noqa: C901, PLR0912, PLR0915
    server: str,
    share: str,
    remote_path: str,
    s3_path: str,
    smb_username: str,
    smb_password: str,
    aws_credentials: dict[str, Any] | None = None,
    prefix_levels_to_add: int = 0,
    organize_by_year: bool = False,
    zip_inner_file_regexes: str | list[str] | None = None,
) -> list[str]:
    """Stream a file directly from SMB server to S3 bucket without saving to disk.

    Uses `smbclient.open_file` to read the remote file and `boto3`'s
    `upload_fileobj` to stream the bytes to S3 without writing to local disk.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        remote_path (str): Path to the file on SMB server (relative to share root).
        s3_path (str): Full S3 path where the file will be uploaded
            (e.g., "s3://bucket/path/").
        smb_username (str): SMB username.
        smb_password (str): SMB password.
        aws_credentials (dict[str, Any] | None): Optional AWS credential mapping
            used to configure the boto3 S3 client.
        prefix_levels_to_add (int): Number of parent folder levels (from the remote
            path) to prepend as an underscore-separated prefix to the S3 filename.
            Example: for remote path "2025/02/file.xlsx" and levels=2, S3 file name
            will be "2025_02_file.xlsx". Defaults to 0 (no prefix).
        organize_by_year (bool): If True and a year (1900-2099) is found in the
            remote_path, organize the file in S3 under a folder named after the year.
            Example: for remote path "2024/data/file.xlsx", S3 key will be
            "prefix/2024/file.xlsx" instead of "prefix/file.xlsx". Defaults to False.
        zip_inner_file_regexes (str | list[str] | None): Regular expression string
            or list of regex patterns used to filter files *inside* ZIP archives.
            If provided and the file is a ZIP, only matching inner files will be
            extracted and streamed to S3. Defaults to None.

    Returns:
        list[str]: A list of S3 paths where files were uploaded. For regular files,
            returns a list with one path. For ZIP files with filtering, returns
            a list of paths for all extracted files.

    Raises:
        SMBFileOperationError: If the streaming operation fails.
        SMBInvalidFilenameError: If filename contains problematic characters.
    """
    logger.info(
        f"Streaming file from SMB to S3: {remote_path} from {server}/{share} -> {s3_path}"
    )

    _ensure_smb_session(server=server, username=smb_username, password=smb_password)

    filename = remote_path.split("\\")[-1].split("/")[-1]
    if prefix_levels_to_add and prefix_levels_to_add > 0:
        prefix = _build_prefix_from_path(remote_path, prefix_levels_to_add)
        filename = _add_prefix_to_filename(filename, prefix)

    if _check_filename_for_problematic_chars(filename):
        error_msg = (
            f"Skipping file with problematic characters in name: {filename} "
            f"(path: {remote_path})"
        )
        logger.warning(error_msg)
        raise SMBInvalidFilenameError(error_msg)

    aws_credentials = aws_credentials or {}

    parsed_s3 = urlparse(s3_path)
    if parsed_s3.scheme != "s3" or not parsed_s3.netloc:
        msg = f"Invalid s3_path: {s3_path}. Expected format like s3://bucket/prefix/."
        raise SMBFileOperationError(msg)

    s3_bucket = parsed_s3.netloc
    s3_base_path = parsed_s3.path.lstrip("/")

    # Optionally organize by year if found in path
    year_folder = ""
    if organize_by_year:
        year = extract_year_from_path(remote_path)
        if year:
            year_folder = f"{year}/"

    s3_key = (
        f"{s3_base_path.rstrip('/')}/{year_folder}{filename}"
        if s3_base_path
        else f"{year_folder}{filename}"
    )
    s3_full_path = f"s3://{s3_bucket}/{s3_key}"

    client_kwargs: dict[str, Any] = {}
    if aws_credentials.get("aws_access_key_id"):
        client_kwargs["aws_access_key_id"] = aws_credentials["aws_access_key_id"]
    if aws_credentials.get("aws_secret_access_key"):
        client_kwargs["aws_secret_access_key"] = aws_credentials[
            "aws_secret_access_key"
        ]
    if aws_credentials.get("aws_session_token"):
        client_kwargs["aws_session_token"] = aws_credentials["aws_session_token"]
    if aws_credentials.get("region_name"):
        client_kwargs["region_name"] = aws_credentials["region_name"]
    elif aws_credentials.get("region"):
        client_kwargs["region_name"] = aws_credentials["region"]
    if aws_credentials.get("endpoint_url"):
        client_kwargs["endpoint_url"] = aws_credentials["endpoint_url"]

    s3_client = boto3.client("s3", **client_kwargs)

    unc_remote_path = _to_unc_path(
        server=server, share=share, relative_path=remote_path
    )

    is_zip_with_filter = filename.lower().endswith(".zip") and zip_inner_file_regexes

    if is_zip_with_filter:
        s3_paths = []
        try:
            with (
                smbclient.open_file(unc_remote_path, mode="rb") as remote_file,
                zipfile.ZipFile(remote_file) as zf,
            ):
                for zip_member_name in zf.namelist():
                    if zip_member_name.endswith("/"):
                        continue

                    # Check if file matches any of the regex patterns
                    if _matches_any_regex_module(
                        zip_member_name, zip_inner_file_regexes
                    ):
                        zip_base_name = Path(zip_member_name).name

                        zip_filename = zip_base_name
                        if prefix_levels_to_add and prefix_levels_to_add > 0:
                            zip_prefix = _build_prefix_from_path(
                                remote_path, prefix_levels_to_add
                            )
                            zip_filename = _add_prefix_to_filename(
                                zip_base_name, zip_prefix
                            )

                        if _check_filename_for_problematic_chars(zip_filename):
                            logger.warning(
                                f"Skipping ZIP file with problematic characters: {zip_filename}"
                            )
                            continue

                        zip_s3_key = (
                            f"{s3_base_path.rstrip('/')}/{year_folder}{zip_filename}"
                            if s3_base_path
                            else f"{year_folder}{zip_filename}"
                        )
                        zip_s3_full_path = f"s3://{s3_bucket}/{zip_s3_key}"

                        try:
                            with zf.open(zip_member_name) as member_file:
                                s3_client.upload_fileobj(
                                    member_file, s3_bucket, zip_s3_key
                                )
                            s3_paths.append(zip_s3_full_path)
                            logger.info(
                                f"Streamed ZIP member to S3: {zip_s3_full_path}"
                            )
                        except ClientError as e:
                            error_code = (
                                e.response.get("Error", {}).get("Code")
                                if hasattr(e, "response")
                                else None
                            )
                            is_access_denied = str(error_code).lower() in {
                                "accessdenied",
                                "access_denied",
                            }
                            error_msg = f"AWS S3 upload failed for ZIP member. S3 path: {zip_s3_full_path}. Error: {e}"
                            if is_access_denied:
                                error_msg += (
                                    " This appears to be an AWS IAM permissions issue. "
                                    "Please ensure credentials have s3:PutObject for the bucket/key."
                                )
                            logger.exception(error_msg)
                            continue
                        except Exception as e:
                            error_msg = f"Unexpected error streaming ZIP member {zip_member_name}: {e}"
                            logger.exception(error_msg)
                            continue

                if not s3_paths:
                    logger.warning(f"No matching files found in ZIP: {remote_path}")

        except zipfile.BadZipFile:
            error_msg = f"Invalid ZIP file: {remote_path}"
            logger.exception(error_msg)
            raise SMBFileOperationError(error_msg) from None
        except Exception as e:
            error_msg = f"Unexpected error during ZIP processing: {e}"
            logger.exception(error_msg)
            raise SMBFileOperationError(error_msg) from e

        return s3_paths
    try:
        with smbclient.open_file(unc_remote_path, mode="rb") as remote_file:
            # upload_fileobj streams in chunks; does not read entire file into memory
            s3_client.upload_fileobj(remote_file, s3_bucket, s3_key)
    except ClientError as e:
        error_code = (
            e.response.get("Error", {}).get("Code") if hasattr(e, "response") else None
        )
        is_access_denied = str(error_code).lower() in {
            "accessdenied",
            "access_denied",
        }
        error_msg = f"AWS S3 upload failed. S3 path: {s3_full_path}. Error: {e}"
        if is_access_denied:
            error_msg += (
                " This appears to be an AWS IAM permissions issue. "
                "Please ensure credentials have s3:PutObject for the bucket/key."
            )
        logger.exception(error_msg)
        raise SMBFileOperationError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error during SMB to S3 streaming: {e}"
        logger.exception(error_msg)
        raise SMBFileOperationError(error_msg) from e

    logger.info(f"Successfully streamed file to S3: {s3_full_path}")
    return [s3_full_path]


def stream_smb_files_to_s3(
    smb_file_paths: list[str],
    smb_server: str,
    smb_share: str,
    s3_path: str,
    smb_username: str,
    smb_password: str,
    aws_credentials: dict[str, Any] | None = None,
    prefix_levels_to_add: int = 0,
    organize_by_year: bool = False,
    zip_inner_file_regexes: str | list[str] | None = None,
) -> list[str]:
    """Stream multiple files directly from SMB server to S3 bucket.

    Streams files from SMB to S3 without saving them to local disk first,
    which is more efficient for large files and reduces disk I/O. Files with
    problematic characters in their names will be skipped.

    Args:
        smb_file_paths (list[str]): List of SMB file paths to stream
            (relative to share root).
        smb_server (str): SMB server address.
        smb_share (str): SMB share name.
        s3_path (str): Base S3 path where files will be uploaded
            (e.g., "s3://bucket/path/"). Filenames will be appended automatically,
            optionally with prefix.
        smb_username (str): SMB username.
        smb_password (str): SMB password.
        aws_credentials (dict[str, Any] | None): Optional AWS credential mapping
            used by the AWS CLI. Must include `aws_access_key_id` and
            `aws_secret_access_key`. Can also include `aws_session_token`,
            `region_name`, and `endpoint_url`.
        prefix_levels_to_add (int): Number of parent folder levels
            (from the remote SMB path) to prepend as an underscore-separated
            prefix to the S3 filename. Example: for path "2025/02/file.xlsx"
            and levels=2, filename will be "2025_02_file.xlsx".
            Defaults to 0 (no prefix).
        organize_by_year (bool): If True and a year (1900-2099) is found in the
            remote_path, organize the file in S3 under a folder named after the year.
            Example: for remote path "2024/data/file.xlsx", S3 key will be
            "prefix/2024/file.xlsx" instead of "prefix/file.xlsx". Defaults to False.
        zip_inner_file_regexes (str | list[str] | None): Regular expression string
            or list of regex patterns used to filter files *inside* ZIP archives.
            If provided and a file is a ZIP, only matching inner files will be
            extracted and streamed to S3. Defaults to None.

    Returns:
        list[str]: List of S3 paths where files were successfully uploaded.
            Files that were skipped (e.g., due to invalid filenames) are
            not included in this list.

    Raises:
        SMBConnectionError: If SMB connection fails.
        SMBFileOperationError: If S3 upload fails.
        SMBInvalidFilenameError: If filename contains problematic characters.
    """
    s3_paths: list[str] = []

    for smb_file_path in smb_file_paths:
        try:
            s3_path_result = _stream_file_from_smb_to_s3(
                server=smb_server,
                share=smb_share,
                remote_path=smb_file_path,
                s3_path=s3_path,
                smb_username=smb_username,
                smb_password=smb_password,
                aws_credentials=aws_credentials,
                prefix_levels_to_add=prefix_levels_to_add,
                organize_by_year=organize_by_year,
                zip_inner_file_regexes=zip_inner_file_regexes,
            )
            s3_paths.extend(s3_path_result)
        except SMBInvalidFilenameError:
            logger.warning(f"Skipping file with invalid filename: {smb_file_path}")
            continue
        except Exception as e:
            logger.warning(
                f"Failed to stream {smb_file_path} to S3: {e}. Skipping file."
            )
            continue

    return s3_paths


def _get_shallow_listing(
    server: str,
    share: str,
    directory: str,
    username: str,
    password: str,
) -> list[SMBItem]:
    """Get shallow (single-level) SMB directory listing.

    Lists only the immediate contents of a directory without recursing into
    subdirectories.
    Directories are marked with needs_expansion=True for later recursive processing.

    Args:
        server (str): SMB server hostname or IP address.
        share (str): SMB share name.
        directory (str): Directory to list (relative to share root).
        username (str): SMB username.
        password (str): SMB password.

    Returns:
        list[SMBItem]: List of SMBItem objects for immediate directory contents.
        Directories have needs_expansion=True.

    Raises:
        SMBConnectionError: If SMB connection or listing fails.
    """
    _ensure_smb_session(server=server, username=username, password=password)

    dir_rel = (directory or "").strip("/").strip("\\")
    unc_dir = _to_unc_path(server=server, share=share, relative_path=dir_rel)
    items: list[SMBItem] = []

    try:
        entries = smbclient.scandir(unc_dir)
    except Exception as e:
        msg = f"Failed to list SMB directory: {dir_rel or ''} ({server}/{share}): {e}"
        raise SMBConnectionError(msg) from e

    for entry in entries:
        name = entry.name
        if name in (".", ".."):
            continue

        rel_path = _to_rel_path(dir_rel, name)

        try:
            st = entry.stat()
            date_modified = datetime.fromtimestamp(st.st_mtime) if st else None
            size = int(st.st_size) if st else None
        except Exception:
            date_modified = None
            size = None

        item_type = SMBItemType.DIRECTORY if entry.is_dir() else SMBItemType.FILE
        needs_expansion = item_type == SMBItemType.DIRECTORY  # Mark dirs for expansion

        smb_item = SMBItem(
            name=name,
            item_type=item_type,
            path=rel_path,
            date_modified=date_modified,
            size=size,
            children=[],  # Empty for shallow listing
            needs_expansion=needs_expansion,
        )

        items.append(smb_item)

    return items


def get_listing_with_shallow_first(
    server: str,
    share: str,
    start_directory: str,
    username: str,
    password: str,
    recursive_timeout: int = 300,
) -> list[SMBItem]:
    """Get SMB directory listing starting with shallow listing, then expanding.

    Always starts with shallow listing at root level, then uses hybrid fallback
    strategy for each subdirectory found.

    Args:
        server (str): SMB server hostname or IP address.
        share (str): SMB share name.
        start_directory (str): Starting directory relative to share root.
        username (str): SMB username.
        password (str): SMB password.
        recursive_timeout (int): Timeout in seconds for subdirectory operations.
            Defaults to 300.

    Returns:
        list[SMBItem]: List of top-level SMBItem objects with expanded subdirectories.
    """
    logger.info(f"Starting shallow-first SMB listing from {start_directory}")

    _ensure_smb_session(server=server, username=username, password=password)

    try:
        root_items = _get_shallow_listing(
            server=server,
            share=share,
            directory=start_directory,
            username=username,
            password=password,
        )
        logger.info(f"Root shallow listing completed with {len(root_items)} items")

        # For each directory found at root level, try hybrid listing
        for item in root_items:
            if item.type == SMBItemType.DIRECTORY:
                try:
                    logger.debug(f"Trying hybrid listing for subdirectory: {item.path}")
                    sub_items = get_hybrid_listing_with_fallback(
                        server=server,
                        share=share,
                        start_directory=item.path,
                        username=username,
                        password=password,
                        recursive_timeout=recursive_timeout,
                    )
                    item.children = sub_items
                    item.needs_expansion = False
                    logger.debug(
                        f"Successfully expanded directory: {item.path} with {len(sub_items)} items"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to expand directory {item.path} with hybrid listing: {e}"
                    )
                    item.needs_expansion = True
                    continue

        logger.info(f"Shallow-first listing completed for {start_directory}")
        return root_items

    except Exception as e:
        logger.warning(f"Root shallow listing failed: {e}, returning empty list")
        return []


def get_hybrid_listing_with_fallback(  # noqa: C901, PLR0915
    server: str,
    share: str,
    start_directory: str,
    username: str,
    password: str,
    recursive_timeout: int = 300,
) -> list[SMBItem]:
    """Get SMB directory listing using a pure-Python approach with automatic fallback.

    Builds a tree structure of SMBItem objects by recursively traversing directories
    using `smbclient.scandir`. Uses threading with timeout to prevent hanging on
    large or problematic directories. If recursive listing fails or times out,
    automatically falls back to shallow listing (single level) with subsequent
    recursive expansion of all directories to ensure complete results.

    Args:
        server (str): SMB server hostname or IP address.
        share (str): SMB share name.
        start_directory (str): Starting directory relative to share root.
        username (str): SMB username.
        password (str): SMB password.
        recursive_timeout (int): Timeout in seconds for the entire operation.
            Defaults to 300.

    Returns:
        list[SMBItem]: List of top-level SMBItem objects representing the complete
        directory structure. Each SMBItem contains children for subdirectories.
        The tree is always complete thanks to automatic fallback and expansion.

    Raises:
        SMBConnectionError: If SMB connection fails completely.

    Notes:
        - The original implementation used an smbclient CLI "recurse; ls" approach.
          This version uses pure Python libraries for better reliability.
        - Uses threading with timeout to prevent hanging on problematic directories.
        - Automatic fallback ensures complete results even for large directory trees.
    """
    logger.info(f"Starting SMB listing from {start_directory}")

    _ensure_smb_session(server=server, username=username, password=password)

    result_container: dict[str, list[SMBItem]] = {}
    exception_container: dict[str, Exception] = {}

    start_rel = (start_directory or "").strip("/").strip("\\")

    def build_tree(dir_rel: str) -> list[SMBItem]:
        """Recursively build a tree of SMBItem objects for the given directory.

        Args:
            dir_rel (str): Relative path to the directory to process.

        Returns:
            list[SMBItem]: List of items in the directory with children populated
            recursively.
        """
        unc_dir = _to_unc_path(server=server, share=share, relative_path=dir_rel)
        items: list[SMBItem] = []

        try:
            entries = smbclient.scandir(unc_dir)
        except Exception as e:
            msg = (
                f"Failed to list SMB directory: {dir_rel or ''} ({server}/{share}): {e}"
            )
            raise SMBConnectionError(msg) from e

        for entry in entries:
            name = entry.name
            if name in (".", ".."):
                continue

            rel_path = _to_rel_path(dir_rel, name)

            try:
                st = entry.stat()
                date_modified = datetime.fromtimestamp(st.st_mtime) if st else None
                size = int(st.st_size) if st else None
            except Exception:
                date_modified = None
                size = None

            item_type = SMBItemType.DIRECTORY if entry.is_dir() else SMBItemType.FILE
            smb_item = SMBItem(
                name=name,
                item_type=item_type,
                path=rel_path,
                date_modified=date_modified,
                size=size,
                children=[],
            )

            if smb_item.type == SMBItemType.DIRECTORY:
                smb_item.children = build_tree(rel_path)

            items.append(smb_item)

        return items

    def run_listing() -> None:
        """Run the directory listing operation with error handling."""
        try:
            result_container["items"] = build_tree(start_rel)
        except Exception as e:
            exception_container["error"] = e

    listing_thread = threading.Thread(target=run_listing, daemon=True)
    listing_thread.start()
    listing_thread.join(timeout=recursive_timeout)

    if not listing_thread.is_alive() and "error" not in exception_container:
        return result_container.get("items", [])

    # Recursive listing failed or timed out - always try fallback
    if listing_thread.is_alive():
        logger.warning(
            f"SMB recursive listing timed out after {recursive_timeout}s, "
            f"falling back to shallow listing with expansion "
            f"(server={server}, share={share}, start_directory={start_directory})"
        )
    else:
        logger.warning(
            f"SMB recursive listing failed, falling back to shallow listing with expansion "
            f"(server={server}, share={share}, start_directory={start_directory})"
        )

    # Force terminate the recursive thread (daemon threads terminate automatically)
    # Do shallow listing first, then try hybrid listing for each subdirectory
    try:
        shallow_items = _get_shallow_listing(
            server=server,
            share=share,
            directory=start_directory,
            username=username,
            password=password,
        )
        logger.info(
            f"Shallow listing completed with {len(shallow_items)} items, expanding directories with hybrid listing..."
        )

        # For each directory found in shallow listing, try hybrid listing
        for item in shallow_items:
            if item.type == SMBItemType.DIRECTORY:
                try:
                    logger.debug(f"Trying hybrid listing for subdirectory: {item.path}")
                    sub_items = get_hybrid_listing_with_fallback(
                        server=server,
                        share=share,
                        start_directory=item.path,
                        username=username,
                        password=password,
                        recursive_timeout=recursive_timeout,  # Same timeout for subdirectories
                    )
                    item.children = sub_items
                    item.needs_expansion = False
                    logger.debug(
                        f"Successfully expanded directory: {item.path} with {len(sub_items)} items"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to expand directory {item.path} with hybrid listing: {e}"
                    )
                    item.needs_expansion = True
                    continue

        logger.info(f"Fallback listing completed for {start_directory}")
        return shallow_items

    except Exception:
        logger.exception("Fallback listing also failed")

    if listing_thread.is_alive():
        msg = (
            f"SMB directory listing timed out after {recursive_timeout}s "
            f"(server={server}, share={share}, start_directory={start_directory})"
        )
        raise SMBConnectionError(msg)

    if "error" in exception_container:
        e = exception_container["error"]
        if isinstance(e, SMBConnectionError):
            raise e
        raise SMBConnectionError(str(e)) from e

    return result_container.get("items", [])


def _matches_any_regex_module(
    text: str,
    patterns: str | list[str] | None = None,
) -> bool:
    """Check whether the given text matches any of the provided regex pattern(s).

    Module-level version for use in static functions.

    Args:
        text (str): The string to test.
        patterns (str | list[str] | None): A regex pattern or list of patterns.
            If None, this method returns True (no filtering applied).

    Returns:
        bool: True if the text matches at least one pattern, or if no pattern
            is given.
    """
    if not patterns:
        return True

    if isinstance(patterns, str):
        patterns = [patterns]

    return any(_safe_regex_match_module(pattern, text) for pattern in patterns)


def _safe_regex_match_module(pattern: str, text: str) -> bool:
    """Evaluate whether a regex pattern matches given text (case-insensitive).

    Module-level version for use in static functions.

    Args:
        pattern (str): The regular expression pattern to match against.
        text (str): The input string to search within.

    Returns:
        bool: True if the pattern matches the text; False if it does not match
            or if the pattern is invalid.
    """
    try:
        return re.search(pattern, text, re.IGNORECASE) is not None
    except re.error as e:
        logger.warning(f"Invalid regex pattern: {pattern} — Error: {e}")
        return False


class SMBClient(Source):
    """Lightweight `Source` connector built around helpers in this module.

    This class does **not** introduce new complex logic (such as full tree scanning),
    it only wraps the existing helper functions:

    - `get_listing_with_shallow_first` - shallow-first directory listing with recursive
    expansion,
    - `download_file_from_smb` - file download with optional ZIP extraction,
    - `stream_smb_files_to_s3` - streaming files directly to S3.
    """

    def __init__(
        self,
        server: str,
        share: str,
        smb_credentials: SMBCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the SMB client using the `smbclient` Python library.

        Args:
            server (str): SMB server host name or IP.
            share (str): SMB share name on the server.
            smb_credentials (SMBCredentials | None): SMB credentials (same structure
                as for the `SMB` source). If None, they can be loaded from config.
            config_key (str | None): Optional key in viadot config. If provided and
                `smb_credentials` is None, SMB creds will be read from config
                (`username` and `password` fields).
            *args: Additional positional arguments passed to the parent Source class.
            **kwargs: Additional keyword arguments passed to the parent Source class.
        """
        raw_creds = smb_credentials or get_source_credentials(config_key) or {}

        # Handle both dict and SMBCredentials object
        if isinstance(raw_creds, SMBCredentials):
            validated_creds = raw_creds
        else:
            validated_creds = SMBCredentials(**raw_creds)

        if validated_creds is None:
            msg = "`smb_credentials` must be provided either directly or via `config_key`."
            raise CredentialError(msg)
        super().__init__(*args, credentials=validated_creds.model_dump(), **kwargs)
        self.server = server
        self.share = share

    def list_directory(
        self,
        directory: str = "",
        recursive_timeout: int = 300,
    ) -> list[SMBItem]:
        """Return directory structure as a list of `SMBItem`.

        Uses a pure-Python recursive approach with `smbclient.scandir`.

        Args:
            directory (str): Start directory relative to the share root
                (e.g. `""` or `"data/reports"`). Defaults to `""` (share root).
            recursive_timeout (int): Timeout in seconds for recursive listing
                attempts. Defaults to 300.

        Returns:
            list[SMBItem]: List of `SMBItem` objects representing the directory
                structure. Items are organized in a tree structure with
                directories containing their children in the `children` attribute.
        """
        return get_listing_with_shallow_first(
            server=self.server,
            share=self.share,
            start_directory=directory,
            username=self.credentials.get("username"),
            password=self.credentials.get("password").get_secret_value(),
            recursive_timeout=recursive_timeout,
        )

    def download_file(
        self,
        remote_path: str,
        local_path: str = "/tmp/smb_files",  # noqa: S108
        timeout: int = 900,
        prefix_levels_to_add: int = 0,
        zip_inner_file_regexes: str | list[str] | None = None,
    ) -> list[str] | None:
        """Download a single file from SMB to local disk.

        For ZIP files, if `zip_inner_file_regexes` is provided, extracts and saves
        only files matching the regex patterns. Otherwise, downloads the entire file.

        If the filename contains problematic characters, the file
        will be skipped and None will be returned.

        Args:
            remote_path (str): Path to the file relative to the share root.
            local_path (str): Local directory where the file should be saved.
                Defaults to "/tmp/smb_files".
            timeout (int): SMB operation timeout in seconds. Defaults to 900.
            prefix_levels_to_add (int): Number of parent directory levels to
                prepend as an underscore-separated prefix to the local filename.
                Example: for remote path "2025/02/file.xlsx" and levels=2,
                local filename will be "2025_02_file.xlsx". Defaults to 0.
            zip_inner_file_regexes (str | list[str] | None): Regular expression
                string or list of regex patterns used to filter files *inside* ZIP
                archives. If provided and the file is a ZIP, only matching inner
                files will be extracted and saved locally. Defaults to None.

        Returns:
            list[str] | None: A list of local paths to downloaded/extracted files.
                For regular files, returns a list with one path. For ZIP files with
                filtering, returns a list of paths to all extracted files that matched
                the regex patterns. Returns None if the file was skipped due to invalid
                filename characters.

        Raises:
            SMBFileOperationError: If the download fails.
        """
        try:
            return download_file_from_smb(
                server=self.server,
                share=self.share,
                remote_path=remote_path,
                username=self.credentials.get("username"),
                password=self.credentials.get("password").get_secret_value(),
                local_path=local_path,
                timeout=timeout,
                prefix_levels_to_add=prefix_levels_to_add,
                zip_inner_file_regexes=zip_inner_file_regexes,
            )
        except SMBInvalidFilenameError:
            logger.warning(f"Skipping file with invalid filename: {remote_path}")
            return None

    def stream_files_to_s3(
        self,
        smb_file_paths: list[str],
        s3_path: str,
        aws_credentials: dict[str, Any],
        prefix_levels_to_add: int = 0,
        organize_by_year: bool = False,
        zip_inner_file_regexes: str | list[str] | None = None,
    ) -> list[str]:
        """Stream multiple files from SMB directly to S3.

        Streams files from SMB to S3 without saving them to local disk first,
        which is more efficient for large files. Files with problematic characters in
        their names will be skipped.

        Args:
            smb_file_paths (list[str]): List of file paths relative to the
                share root.
            s3_path (str): Base S3 path (e.g. ``s3://bucket/folder/``).
                Filenames will be appended automatically, optionally with prefix.
            aws_credentials (dict[str, Any]): Required AWS credential mapping
                used by the AWS CLI. Must include `aws_access_key_id` and
                `aws_secret_access_key`. Can also include `aws_session_token`,
                `region_name`, and `endpoint_url`.
            prefix_levels_to_add (int): Number of parent directory levels to
                prepend as an underscore-separated prefix to the S3 object name.
                Example: for remote path "2025/02/file.xlsx" and levels=2,
                S3 object name will be "2025_02_file.xlsx". Defaults to 0.
            organize_by_year (bool): If True and a year (1900-2099) is found in the
                remote_path, organize the file in S3 under a folder named after
                the year.
                Example: for remote path "2024/data/file.xlsx", S3 key will be
                "prefix/2024/file.xlsx" instead of "prefix/file.xlsx".
                Defaults to False.
            zip_inner_file_regexes (str | list[str] | None): Regular expression
                string or list of regex patterns used to filter files *inside* ZIP
                archives. If provided and a file is a ZIP, only matching inner
                files will be extracted and streamed to S3. Defaults to None.

        Returns:
            list[str]: List of S3 paths where files were successfully uploaded.
                Files that were skipped (e.g., due to invalid filenames) are
                not included in this list.

        Raises:
            CredentialError: If `aws_credentials` is not a dictionary, is empty,
                or missing required fields (`aws_access_key_id` and
                `aws_secret_access_key`).
        """
        if not isinstance(aws_credentials, dict):
            msg = "`aws_credentials` must be a dictionary."
            raise CredentialError(msg)

        if not (
            aws_credentials.get("aws_access_key_id")
            and aws_credentials.get("aws_secret_access_key")
        ):
            msg = (
                "`aws_credentials` must include "
                "`aws_access_key_id` and `aws_secret_access_key`."
            )
            raise CredentialError(msg)

        return stream_smb_files_to_s3(
            smb_file_paths=smb_file_paths,
            smb_server=self.server,
            smb_share=self.share,
            s3_path=s3_path,
            smb_username=self.credentials.get("username"),
            smb_password=self.credentials.get("password").get_secret_value(),
            aws_credentials=aws_credentials,
            prefix_levels_to_add=prefix_levels_to_add,
            organize_by_year=organize_by_year,
            zip_inner_file_regexes=zip_inner_file_regexes,
        )
