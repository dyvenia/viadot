"""Helpers and a lightweight Source wrapper around the `smbclient` CLI."""

from collections.abc import Callable
from datetime import datetime
from enum import Enum
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from typing import Any
from urllib.parse import urlparse

import pendulum
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.orchestration.prefect.utils import DynamicDateHandler
from viadot.sources.base import Source
from viadot.sources.smb import SMBCredentials


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
    logger = get_run_logger()
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


class SMBError(Exception):
    """Base exception for SMB operations."""

    pass


class SMBConnectionError(SMBError):
    """Exception raised when SMB connection fails."""

    pass


class SMBFileOperationError(SMBError):
    """Exception raised when SMB file operations fail."""

    pass


class SMBInvalidFilenameError(SMBError):
    """Exception raised when filename contains invalid/problematic characters."""

    pass


class SMBItemType(Enum):
    DIRECTORY = "D"
    FILE = "A"
    # Add other types as needed based on smbclient output


class SMBItem:
    def __init__(
        self,
        name: str,
        item_type: SMBItemType,
        path: str = "",
        date_modified: datetime | None = None,
        size: int | None = None,
        children: list["SMBItem"] | None = None,
    ):
        """Initialize SMB item with name, type, path, date, size, and children."""
        self.name = name
        self.type = item_type
        self.path = path  # Full path to this item
        self.date_modified = date_modified  # Modification date
        self.size = size  # File size in bytes
        self.children = children or []

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
        logger = get_run_logger()
        try:
            return re.search(pattern, text, re.IGNORECASE) is not None
        except re.error as e:
            # Use module-level logger since SMBItem does not define an instance logger
            logger.warning(f"Invalid regex pattern: {pattern} — Error: {e}")
            return False

    def _is_date_match(
        self,
        file_modification_date: pendulum.Date | None,
        date_filter_parsed: pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None,
    ) -> bool:
        """Check if the file modification date matches the given date filter.

        Args:
            file_modification_date (pendulum.Date | None): The modification date of the
                file. If None, the file will not match any date filter.
            date_filter_parsed (
                pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None
            ):
                - A single `pendulum.Date` for exact date filtering.
                - A tuple of two `pendulum.Date` values for date range filtering.
                - None, if no date filter is applied.

        Returns:
            bool: True if the file_modification_date matches the filter or if no filter
                is applied. False otherwise.
        """
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

        # Decide which regex filter to apply:
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


def _retry_with_exponential_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    *args,
    **kwargs,
) -> Any:  # noqa: ANN401
    """Retry a function with exponential backoff for SMB operations.

    Args:
        func (Callable): Function to retry.
        max_retries (int): Maximum number of retry attempts. Defaults to 3.
        base_delay (float): Initial delay in seconds. Defaults to 1.0.
        max_delay (float): Maximum delay between retries. Defaults to 60.0.
        backoff_factor (float): Factor by which delay increases. Defaults to 2.0.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        Result of the function call

    Raises:
        Last exception encountered after all retries are exhausted
    """
    logger = get_run_logger()
    last_exception = None

    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            return func(*args, **kwargs)
        except (SMBConnectionError, SMBFileOperationError) as e:
            last_exception = e

            if attempt == max_retries:
                logger.exception(f"All {max_retries + 1} attempts failed")
                raise

            delay = min(base_delay * (backoff_factor**attempt), max_delay)
            logger.warning(
                f"SMB operation failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                f"Retrying in {delay:.1f} seconds..."
            )
            time.sleep(delay)

    # This should never be reached, but just in case
    if last_exception:
        raise last_exception

    return None  # type: ignore


def _download_file_from_smb(
    server: str,
    share: str,
    remote_path: str,
    username: str,
    password: str,
    local_path: str = "/tmp",  # noqa: S108
    timeout: int = 900,
    prefix_levels_to_add: int = 0,
) -> str:
    """Download a file from SMB server to local filesystem.

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

    Returns:
        str: Full local path to the downloaded file.

    Raises:
        SMBFileOperationError: If the download fails.
        SMBInvalidFilenameError: If filename contains problematic characters.
    """
    logger = get_run_logger()
    logger.info(f"Downloading file from SMB: {remote_path} from {server}/{share}")

    # Extract filename from remote path
    filename = remote_path.split("\\")[-1].split("/")[-1]

    # Optionally build prefix from parent directories of the remote path
    if prefix_levels_to_add and prefix_levels_to_add > 0:
        prefix = _build_prefix_from_path(remote_path, prefix_levels_to_add)
        filename = _add_prefix_to_filename(filename, prefix)

    # Check for problematic characters in filename
    if _check_filename_for_problematic_chars(filename):
        error_msg = f"Skipping file with problematic characters in name: {filename} (path: {remote_path})"
        logger.warning(error_msg)
        raise SMBInvalidFilenameError(error_msg)

    Path(local_path).mkdir(parents=True, exist_ok=True)
    full_local_path = _get_unique_local_path(local_path.rstrip("/"), filename)

    cmd = [
        "smbclient",
        f"//{server}/{share}",
        "-U",
        f"{username}%{password}",
        "-t",
        str(timeout),
        "-c",
        f'get "{remote_path}" "{full_local_path}"',
    ]

    logger.debug(
        f'Executing SMB download command: smbclient //{server}/{share} -U {username}*** -t {timeout} -c \'get "{remote_path}" "{full_local_path}"\''
    )

    try:
        result = subprocess.run(  # noqa: S603
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        error_msg = f"SMB download timed out after {timeout} seconds"
        logger.exception(error_msg)
        raise SMBFileOperationError(error_msg) from e

    if result.returncode != 0:
        stdout_preview = result.stdout[:500] if result.stdout else "No stdout"
        stderr_content = result.stderr.strip() if result.stderr else "No stderr output"

        error_msg = (
            f"SMB download failed with return code {result.returncode}. "
            f"Server: {server}, Share: {share}, Path: {remote_path}. "
            f"Stderr: {stderr_content}. "
            f"Stdout preview: {stdout_preview}"
        )
        logger.error(error_msg)
        raise SMBFileOperationError(error_msg)

    logger.info(f"Successfully downloaded file to: {full_local_path}")
    return full_local_path


def download_file_from_smb_with_retry(
    server: str,
    share: str,
    remote_path: str,
    username: str,
    password: str,
    local_path: str = "/tmp/smb_files",  # noqa: S108
    timeout: int = 900,
    max_retries: int = 3,
    base_delay: float = 2.0,
    prefix_levels_to_add: int = 0,
) -> str | None:
    """Download a file from SMB server with retry mechanism.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        remote_path (str): Path to the file on SMB server (relative to share root).
        username (str): SMB username.
        password (str): SMB password.
        local_path (str): Local directory to save the file.
            Defaults to "/tmp/smb_files".
        timeout (int): Timeout in seconds for the SMB operation. Defaults to 900.
        max_retries (int): Maximum number of retry attempts for failed operations.
            Defaults to 3.
        base_delay (float): Base delay in seconds for exponential backoff.
            Defaults to 2.0.
        prefix_levels_to_add (int): Number of parent folder levels (from the remote
            path) to prepend as an underscore-separated prefix to the local filename.
            Passed through to `_download_file_from_smb`. Defaults to 0 (no prefix).

    Returns:
        str | None: Full local path to the downloaded file, or None if file was
        skipped due to invalid filename.

    Raises:
        SMBFileOperationError: If all retry attempts fail.
    """
    logger = get_run_logger()
    try:
        return _retry_with_exponential_backoff(
            _download_file_from_smb,
            max_retries=max_retries,
            base_delay=base_delay,
            server=server,
            share=share,
            remote_path=remote_path,
            username=username,
            password=password,
            local_path=local_path,
            timeout=timeout,
            prefix_levels_to_add=prefix_levels_to_add,
        )
    except SMBInvalidFilenameError:
        logger.warning(f"Skipping file with invalid filename: {remote_path}")
        return None


def _stream_file_from_smb_to_s3(  # noqa: C901, PLR0912, PLR0915
    server: str,
    share: str,
    remote_path: str,
    s3_path: str,
    smb_username: str,
    smb_password: str,
    aws_credentials: dict[str, Any] | None = None,
    timeout: int = 900,
    prefix_levels_to_add: int = 0,
) -> str:
    """Stream a file directly from SMB server to S3 bucket without saving to disk.

    This function uses a pipe to stream data from smbclient to aws s3 cp, avoiding
    the need to write the file to local disk first. This is more efficient for large
    files and reduces disk I/O.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        remote_path (str): Path to the file on SMB server (relative to share root).
        s3_path (str): Full S3 path where the file will be uploaded
            (e.g., "s3://bucket/path/").
        smb_username (str): SMB username.
        smb_password (str): SMB password.
        aws_credentials (dict[str, Any] | None): Optional AWS credential mapping
            used by the AWS CLI.
        timeout (int): Timeout in seconds for the streaming operation. Defaults to 900.
        prefix_levels_to_add (int): Number of parent folder levels (from the remote
            path) to prepend as an underscore-separated prefix to the S3 filename.
            Example: for remote path "2025/02/file.xlsx" and levels=2, S3 file name
            will be "2025_02_file.xlsx". Defaults to 0 (no prefix).

    Returns:
        str: The S3 path where the file was uploaded.

    Raises:
        SMBFileOperationError: If the streaming operation fails.
        SMBInvalidFilenameError: If filename contains problematic characters.
    """
    logger = get_run_logger()
    logger.info(
        f"Streaming file from SMB to S3: {remote_path} from {server}/{share} -> {s3_path}"
    )

    filename = remote_path.split("\\")[-1].split("/")[-1]
    if prefix_levels_to_add and prefix_levels_to_add > 0:
        prefix = _build_prefix_from_path(remote_path, prefix_levels_to_add)
        filename = _add_prefix_to_filename(filename, prefix)

    if _check_filename_for_problematic_chars(filename):
        error_msg = f"Skipping file with problematic characters in name: {filename} (path: {remote_path})"
        logger.warning(error_msg)
        raise SMBInvalidFilenameError(error_msg)

    aws_credentials = aws_credentials or {}

    parsed_s3 = urlparse(s3_path)
    s3_bucket = parsed_s3.netloc
    s3_base_path = parsed_s3.path.lstrip("/")
    s3_full_path = f"s3://{s3_bucket}/{s3_base_path.rstrip('/')}/{filename}"

    # SMB client command to output file to stdout
    smb_cmd = [
        "smbclient",
        f"//{server}/{share}",
        "-U",
        f"{smb_username}%{smb_password}",
        "-t",
        str(timeout),
        "-c",
        f'get "{remote_path}" -',
    ]

    # AWS S3 command to read from stdin and upload
    aws_cmd = [
        "aws",
        "s3",
        "cp",
        "-",
        s3_full_path,
    ]

    if aws_credentials.get("endpoint_url"):
        aws_cmd.insert(-1, "--endpoint-url")
        aws_cmd.insert(-1, aws_credentials["endpoint_url"])

    # Set AWS credentials as environment variables for AWS CLI
    env = os.environ.copy()
    if "aws_access_key_id" in aws_credentials:
        env["AWS_ACCESS_KEY_ID"] = aws_credentials["aws_access_key_id"]
    if "aws_secret_access_key" in aws_credentials:
        env["AWS_SECRET_ACCESS_KEY"] = aws_credentials["aws_secret_access_key"]
    if "aws_session_token" in aws_credentials:
        env["AWS_SESSION_TOKEN"] = aws_credentials["aws_session_token"]
    if "region_name" in aws_credentials:
        env["AWS_DEFAULT_REGION"] = aws_credentials["region_name"]
    elif "region" in aws_credentials:
        env["AWS_DEFAULT_REGION"] = aws_credentials["region"]

    try:
        # Start smbclient process
        smb_process = subprocess.Popen(  # noqa: S603
            smb_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,  # Use binary mode for streaming
        )

        # Start AWS S3 process with stdin from smbclient
        aws_process = subprocess.Popen(  # noqa: S603
            aws_cmd,
            stdin=smb_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=False,  # Use binary mode for streaming
        )

        smb_process.stdout.close()
        aws_stdout, aws_stderr = aws_process.communicate(timeout=timeout)
        smb_stderr = smb_process.stderr.read()
        smb_process.wait()

        if smb_process.returncode != 0:
            smb_stderr_str = (
                smb_stderr.decode("utf-8", errors="replace")
                if smb_stderr
                else "No stderr output"
            )
            error_msg = (
                f"SMB streaming failed with return code {smb_process.returncode}. "
                f"Server: {server}, Share: {share}, Path: {remote_path}. "
                f"Stderr: {smb_stderr_str}"
            )
            logger.error(error_msg)
            raise SMBFileOperationError(error_msg)  # noqa: TRY301

        if aws_process.returncode != 0:
            aws_stderr_str = (
                aws_stderr.decode("utf-8", errors="replace")
                if aws_stderr
                else "No stderr output"
            )
            aws_stdout_str = (
                aws_stdout.decode("utf-8", errors="replace")
                if aws_stdout
                else "No stdout"
            )

            is_access_denied = (
                "AccessDenied" in aws_stderr_str
                or "not authorized" in aws_stderr_str.lower()
            )
            error_msg = (
                f"AWS S3 upload failed with return code {aws_process.returncode}. "
                f"S3 path: {s3_full_path}. "
            )

            if is_access_denied:
                error_msg += (
                    "This appears to be an AWS IAM permissions issue. "
                    "Please check that the AWS credentials have s3:PutObject permission "
                    "for the bucket and path. "
                )

            error_msg += f"Stderr: {aws_stderr_str}. " f"Stdout: {aws_stdout_str}"

            logger.error(error_msg)
            raise SMBFileOperationError(error_msg)  # noqa: TRY301

    except subprocess.TimeoutExpired as e:
        error_msg = f"SMB to S3 streaming timed out after {timeout} seconds"
        logger.exception(error_msg)

        try:
            smb_process.terminate()
            aws_process.terminate()
        except Exception as cleanup_error:
            logger.debug(f"Error during process cleanup: {cleanup_error}")
        raise SMBFileOperationError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error during SMB to S3 streaming: {e}"
        logger.exception(error_msg)

        try:
            smb_process.terminate()
            aws_process.terminate()
        except Exception as cleanup_error:
            logger.debug(f"Error during process cleanup: {cleanup_error}")
        raise SMBFileOperationError(error_msg) from e

    logger.info(f"Successfully streamed file to S3: {s3_full_path}")
    return s3_full_path


def _stream_file_from_smb_to_s3_with_retry(
    server: str,
    share: str,
    remote_path: str,
    s3_path: str,
    smb_username: str,
    smb_password: str,
    aws_credentials: dict[str, Any] | None = None,
    timeout: int = 900,
    max_retries: int = 3,
    base_delay: float = 2.0,
    prefix_levels_to_add: int = 0,
) -> str | None:
    """Stream a file directly from SMB server to S3 bucket with retry mechanism.

    This function uses a pipe to stream data from smbclient to aws s3 cp, avoiding
    the need to write the file to local disk first. This is more efficient for large
    files and reduces disk I/O.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        remote_path (str): Path to the file on SMB server (relative to share root).
        s3_path (str): Full S3 path where the file will be uploaded
            (e.g., "s3://bucket/path/"). The filename will be appended automatically,
            optionally with prefix.
        smb_username (str): SMB username.
        smb_password (str): SMB password.
        aws_credentials (dict[str, Any] | None): Optional AWS credential mapping
            used by the AWS CLI.
        timeout (int): Timeout in seconds for the streaming operation. Defaults to 900.
        max_retries (int): Maximum number of retry attempts for failed operations.
            Defaults to 3.
        base_delay (float): Base delay in seconds for exponential backoff.
            Defaults to 2.0.
        prefix_levels_to_add (int): Number of parent folder levels (from the remote
            path) to prepend as an underscore-separated prefix to the S3 filename.
            Passed through to `_stream_file_from_smb_to_s3`. Defaults to 0 (no prefix).

    Returns:
        str | None: The S3 path where the file was uploaded, or None if file was
        skipped due to invalid filename.

    Raises:
        SMBFileOperationError: If all retry attempts fail.
    """
    logger = get_run_logger()
    try:
        return _retry_with_exponential_backoff(
            _stream_file_from_smb_to_s3,
            max_retries=max_retries,
            base_delay=base_delay,
            server=server,
            share=share,
            remote_path=remote_path,
            s3_path=s3_path,
            smb_username=smb_username,
            smb_password=smb_password,
            aws_credentials=aws_credentials,
            timeout=timeout,
            prefix_levels_to_add=prefix_levels_to_add,
        )
    except SMBInvalidFilenameError:
        logger.warning(f"Skipping file with invalid filename: {remote_path}")
        return None


def stream_smb_files_to_s3(
    smb_file_paths: list[str],
    smb_server: str,
    smb_share: str,
    s3_path: str,
    smb_username: str,
    smb_password: str,
    aws_credentials: dict[str, Any] | None = None,
    timeout: int = 900,
    max_retries: int = 3,
    base_delay: float = 2.0,
    prefix_levels_to_add: int = 0,
) -> list[str]:
    """Stream multiple files directly from SMB server to S3 bucket.

    This function streams files from SMB to S3 without saving them to local disk,
    which is more efficient for large files and reduces disk I/O.

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
            used by the AWS CLI.
        timeout (int): Timeout in seconds for each streaming operation.
            Defaults to 900.
        max_retries (int): Maximum number of retry attempts for failed operations.
            Defaults to 3.
        base_delay (float): Base delay in seconds for exponential backoff.
            Defaults to 2.0.
        prefix_levels_to_add (int): Number of parent folder levels
            (from the remote SMB path) to prepend as an underscore-separated
            prefix to the S3 filename. Example: for path "2025/02/file.xlsx"
            and levels=2, filename will be "2025_02_file.xlsx".
            Defaults to 0 (no prefix).

    Returns:
        list[str]: List of S3 paths where files were successfully uploaded.
    """
    logger = get_run_logger()
    s3_paths: list[str] = []

    for smb_file_path in smb_file_paths:
        try:
            s3_path_result = _stream_file_from_smb_to_s3_with_retry(
                server=smb_server,
                share=smb_share,
                remote_path=smb_file_path,
                s3_path=s3_path,
                smb_username=smb_username,
                smb_password=smb_password,
                aws_credentials=aws_credentials,
                timeout=timeout,
                max_retries=max_retries,
                base_delay=base_delay,
                prefix_levels_to_add=prefix_levels_to_add,
            )
            if s3_path_result is not None:
                s3_paths.append(s3_path_result)
        except Exception as e:
            logger.warning(
                f"Failed to stream {smb_file_path} to S3: {e}. Skipping file."
            )
            continue

    return s3_paths


def _get_recursive_listing(
    server: str,
    share: str,
    subdirectory: str,
    username: str,
    password: str,
) -> list[str]:
    """Get recursive listing from SMB share using smbclient.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        subdirectory (str): Subdirectory path within the share to list recursively.
        username (str): SMB username.
        password (str): SMB password.

    Returns:
        list[str]: List of lines from smbclient output.

    Raises:
        SMBConnectionError: If the smbclient command fails.
    """
    logger = get_run_logger()
    logger.info(
        f"Getting recursive listing from SMB: {subdirectory} on {server}/{share}"
    )

    cmd = [
        "smbclient",
        f"//{server}/{share}",
        "-U",
        f"{username}%{password}",
        "-c",
        f'cd "{subdirectory}"; recurse; ls',
    ]

    logger.debug(
        f"Executing SMB command: smbclient //{server}/{share} -U {username}*** -c 'cd \"{subdirectory}\"; recurse; ls'"
    )

    try:
        result = subprocess.run(  # noqa: S603
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as e:
        error_msg = "SMB client command timed out after 300 seconds"
        logger.exception(error_msg)
        raise SMBConnectionError(error_msg) from e

    if result.returncode != 0:
        stdout_preview = result.stdout[:500] if result.stdout else "No stdout"
        stderr_content = result.stderr.strip() if result.stderr else "No stderr output"

        error_msg = (
            f"SMB client command failed with return code {result.returncode}. "
            f"Server: {server}, Share: {share}, Subdirectory: {subdirectory}. "
            f"Stderr: {stderr_content}. "
            f"Stdout preview: {stdout_preview}"
        )
        logger.error(error_msg)
        raise SMBConnectionError(error_msg)

    lines = result.stdout.splitlines()
    logger.info(f"Successfully retrieved {len(lines)} lines from SMB listing")

    return lines


def _get_directory_listing_with_fallback(
    server: str,
    share: str,
    directory: str,
    username: str,
    password: str,
    recursive_timeout: int = 300,
) -> tuple[list[SMBItem], bool]:
    """Get directory listing with recursive-first, deep-recursive-fallback approach.

    Always tries recursive first. If recursive succeeds, returns all items recursively.
    If recursive fails/times out, does single-level listing, finds subdirectories, and
    recursively processes each subdirectory with the same logic.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        directory (str): Directory path to list.
        username (str): SMB username.
        password (str): SMB password.
        recursive_timeout (int): Timeout in seconds for recursive listing attempt.
            Defaults to 300.

    Returns:
        tuple[list[SMBItem], bool]: Tuple of (items, was_recursive) where:
        - items: List of `SMBItem` objects from directory and subdirectories.
        - was_recursive: True if recursive listing succeeded, False if fallback
            was used.
    """
    logger = get_run_logger()
    logger.info(f"🔍 Getting directory listing for {directory}")

    try:
        logger.info(f"📁 Trying RECURSIVE listing for {directory} (300s timeout)")
        recursive_start_time = time.time()

        result_container = {}
        exception_container = {}

        def run_recursive():
            try:
                cmd = [
                    "smbclient",
                    f"//{server}/{share}",
                    "-U",
                    f"{username}%{password}",
                    "-c",
                    f'cd "{directory}"; recurse; ls',
                ]

                result = subprocess.run(  # noqa: S603
                    cmd,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=recursive_timeout,
                )

                if result.returncode == 0:
                    lines = result.stdout.splitlines()
                    result_container["lines"] = lines
            except Exception as e:
                exception_container["error"] = e

        recursive_thread = threading.Thread(target=run_recursive)
        recursive_thread.daemon = True
        recursive_thread.start()
        recursive_thread.join(timeout=recursive_timeout)

        recursive_duration = time.time() - recursive_start_time

        if "lines" in result_container:
            logger.info(
                f"✅ Recursive listing SUCCEEDED in {recursive_duration:.1f}s for {directory}"
            )
            tree_items = _parse_recursive_listing(
                result_container["lines"], base_directory=directory
            )
            logger.debug(
                f"Parsed {len(tree_items)} top-level items via recursive listing for {directory}"
            )
            return tree_items, True

    except Exception as e:
        logger.info(f"❌ Recursive listing FAILED for {directory}: {e}")

    logger.info(f"🔄 Falling back to SINGLE-LEVEL + RECURSIVE SUBDIRS for {directory}")
    direct_items = _get_single_level_listing(
        server, share, directory, username, password
    )

    # Build tree: attach subtrees to directory items
    tree_items = []
    for item in direct_items:
        if item.type == SMBItemType.DIRECTORY:
            # For directories, recursively get their subtree and attach as children
            logger.info(f"📂 Recursively processing subdirectory: {item.path}")
            try:
                subdir_tree, _ = _get_directory_listing_with_fallback(
                    server=server,
                    share=share,
                    directory=item.path,
                    username=username,
                    password=password,
                    recursive_timeout=recursive_timeout,
                )
                item.children = subdir_tree  # Attach subtree as children
                logger.info(
                    f"📂 Subdirectory {item.path} got {len(subdir_tree)} children"
                )
            except Exception as e:
                logger.warning(f"Failed to process subdirectory {item.path}: {e}")
                item.children = []  # Empty children on failure
        tree_items.append(item)

    logger.info(f"📊 Built tree with {len(tree_items)} top-level items for {directory}")
    return tree_items, False  # was_recursive = False (fallback was used)


def _get_single_level_listing(
    server: str,
    share: str,
    directory: str,
    username: str,
    password: str,
) -> list[SMBItem]:
    """Get listing of a single directory level (non-recursive).

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        directory (str): Directory path to list (relative to share root).
        username (str): SMB username.
        password (str): SMB password.

    Returns:
        list[SMBItem]: List of `SMBItem` objects in the specified directory.

    Raises:
        SMBConnectionError: If the listing fails.
    """
    logger = get_run_logger()
    logger.info(
        f"Getting single-level listing from SMB: {directory} on {server}/{share}"
    )

    # Use 'ls' without 'recurse' to get only current directory level
    cmd = [
        "smbclient",
        f"//{server}/{share}",
        "-U",
        f"{username}%{password}",
        "-c",
        f'cd "{directory}"; ls',
    ]

    logger.debug(
        f"Executing single-level SMB command: smbclient //{server}/{share} -U {username}*** -c 'cd \"{directory}\"; ls'"
    )

    try:
        result = subprocess.run(  # noqa: S603
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as e:
        error_msg = "SMB single-level listing timed out after 300 seconds"
        logger.exception(error_msg)
        raise SMBConnectionError(error_msg) from e

    if result.returncode != 0:
        stdout_preview = result.stdout[:500] if result.stdout else "No stdout"
        stderr_content = result.stderr.strip() if result.stderr else "No stderr output"

        error_msg = (
            f"SMB single-level listing failed with return code {result.returncode}. "
            f"Server: {server}, Share: {share}, Directory: {directory}. "
            f"Stderr: {stderr_content}. "
            f"Stdout preview: {stdout_preview}"
        )
        logger.error(error_msg)
        raise SMBConnectionError(error_msg)

    lines = result.stdout.splitlines()
    logger.info(f"Retrieved {len(lines)} lines from single-level SMB listing")

    # Parse the lines (skip header lines that may contain directory info)
    items = []
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith(
            "\\"
        ):  # Skip empty lines and directory headers
            continue

        # Parse using the same regex as recursive listing
        match = re.match(
            r"^(.+?)\s+([A-Z])\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)\s+(\d+:\d+:\d+)\s+(\d+)$",
            stripped_line,
        )
        if match:
            name = match.group(1).strip()
            type_char = match.group(2)
            size = int(match.group(3))
            month_name = match.group(5)
            day = match.group(6)
            time_str = match.group(7)
            year = match.group(8)

            try:
                date_string = f"{month_name} {day} {year} {time_str}"
                date_modified = datetime.strptime(date_string, "%b %d %Y %H:%M:%S")
            except ValueError:
                date_modified = None

            if name in (".", ".."):
                continue

            try:
                item_type = SMBItemType(type_char)
            except ValueError:
                item_type = SMBItemType.FILE

            full_path = f"{directory}/{name}" if directory != "/" else f"/{name}"

            item = SMBItem(name, item_type, full_path, date_modified, size)
            items.append(item)

    logger.info(f"Parsed {len(items)} items from directory {directory}")
    return items


def get_hybrid_listing_with_fallback(
    server: str,
    share: str,
    start_directory: str,
    username: str,
    password: str,
    recursive_timeout: int = 300,
    skip_root_recursive: bool = False,
) -> list[SMBItem]:
    """Get SMB listing using hybrid approach: recursive first, then iterative fallback.

    If `skip_root_recursive` is False, tries recursive listing on root first. If it
    succeeds, returns all items. If it fails/times out, falls back to iterative.

    If `skip_root_recursive` is True, directly uses iterative approach.

    In iterative mode, each directory tries recursive first, then single-level.

    Args:
        server (str): SMB server address.
        share (str): SMB share name.
        start_directory (str): Starting directory path.
        username (str): SMB username.
        password (str): SMB password.
        recursive_timeout (int): Timeout in seconds for recursive listing attempts.
            Defaults to 300.
        skip_root_recursive (bool): If True, skip recursive attempt on root and use
            iterative approach. Defaults to False.

    Returns:
        list[SMBItem]: List of all `SMBItem` objects found.
    """
    logger = get_run_logger()
    logger.info(f"Starting hybrid SMB listing from {start_directory}")

    if not skip_root_recursive:
        # First, try recursive listing on root directory with timeout
        try:
            logger.info(
                f"Attempting recursive listing on root directory with {recursive_timeout}s timeout"
            )
            recursive_start_time = time.time()
            result_container = {}
            exception_container = {}

            def run_recursive():
                try:
                    lines = _retry_with_exponential_backoff(
                        _get_recursive_listing,
                        max_retries=1,  # Only one retry for the timeout attempt
                        base_delay=1.0,
                        server=server,
                        share=share,
                        subdirectory=start_directory,
                        username=username,
                        password=password,
                    )
                    result_container["lines"] = lines
                except Exception as e:
                    exception_container["error"] = e

            recursive_thread = threading.Thread(target=run_recursive)
            recursive_thread.daemon = True
            recursive_thread.start()
            recursive_thread.join(timeout=recursive_timeout)

            recursive_duration = time.time() - recursive_start_time

            if "lines" in result_container:
                smb_items = _parse_recursive_listing(
                    result_container["lines"], base_directory=start_directory
                )
                logger.info(
                    f"✅ Root recursive listing SUCCEEDED in {recursive_duration:.1f}s, "
                    f"successfully parsed {len(smb_items)} items"
                )
                return smb_items

            if recursive_thread.is_alive():
                logger.warning(
                    f"❌ Root recursive listing TIMED OUT after {recursive_timeout}s, falling back to checkpointing"
                )
            else:
                logger.warning(
                    f"❌ Root recursive listing FAILED: {exception_container.get('error', 'Unknown error')}, falling back to checkpointing"
                )

        except Exception as e:
            logger.warning(
                f"❌ Root recursive listing attempt failed: {e}, doing single-level on root + checkpointing subdirs"
            )

    # Fallback: do single-level on root directory and process subdirectories recursively
    try:
        logger.info("📂 Doing single-level listing on root directory")
        root_items = _get_single_level_listing(
            server=server,
            share=share,
            directory=start_directory,
            username=username,
            password=password,
        )

        # Separate files and directories
        root_files = [item for item in root_items if item.type == SMBItemType.FILE]
        subdirs = [item for item in root_items if item.type == SMBItemType.DIRECTORY]

        logger.info(
            f"Found {len(root_files)} files and {len(subdirs)} subdirectories in root"
        )

        # Start with root files
        all_items = root_files.copy()

        # For each directory, get its full recursive structure and add as child
        for dir_item in subdirs:
            try:
                logger.info(f"Getting recursive structure for {dir_item.path}")
                dir_tree, _ = _get_directory_listing_with_fallback(
                    server=server,
                    share=share,
                    directory=dir_item.path,
                    username=username,
                    password=password,
                    recursive_timeout=recursive_timeout,  # Full timeout for recursive
                )
                # Attach the tree as children of this directory
                dir_item.children = dir_tree
                all_items.append(dir_item)  # Add directory with its children to result
                logger.info(f"Attached {len(dir_tree)} items to {dir_item.path}")
            except Exception as e:
                logger.warning(f"Failed to get structure for {dir_item.path}: {e}")
                # Add directory without children
                all_items.append(dir_item)
    except Exception:
        logger.exception("Fallback approach failed")
        raise
    else:
        return all_items


def _parse_recursive_listing(  # noqa: C901, PLR0912, PLR0915
    lines: list[str], base_directory: str = ""
) -> list[SMBItem]:
    """Parse smbclient recursive listing output into a tree structure.

    Args:
        lines (list[str]): List of lines from smbclient output.
        base_directory (str): Base directory path to prepend to all item paths.
            This should match the directory that was used in the `cd` command before
            running `recurse; ls` in smbclient. Examples:
            - If listing was done from root share: "" (empty string)
            - If listing was done from "data/reports" directory: "data/reports"
            Defaults to "".

    Returns:
        list[SMBItem]: List of top-level `SMBItem` objects representing the directory
        structure.
    """
    root_items = []
    current_parent = None
    current_path = ""  # Track the current directory path

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            i += 1
            continue

        # Check if this is a directory header (starts with backslash)
        if stripped.startswith("\\"):
            dir_path = stripped
            path_parts = [p for p in dir_path.strip("\\").split("\\") if p]

            # The path includes the full path from share root. Since we cd'ed to
            # base_directory before running the listing, we need to find the directory
            # starting from our root_items. We traverse the path parts to locate the
            # corresponding directory item in our tree structure.
            current_parent = None
            current_path_parts = []
            for part in path_parts:
                search_items = (
                    root_items if current_parent is None else current_parent.children
                )

                for item in search_items:
                    if item.name == part and item.type == SMBItemType.DIRECTORY:
                        current_parent = item
                        current_path_parts.append(part)
                        break

            # Set the current path
            current_path = "/".join(current_path_parts)

            # If we found the directory, set current_parent to it
            # If not found, current_parent remains None (which is fine)
            if current_path:
                current_path = "/" + current_path

            i += 1
            continue

        # Check if this is a file/directory listing line (starts with spaces)
        if line.startswith("  "):
            stripped = line.strip()
            # Use regex to parse smbclient output format: name, type, size, date
            # Pattern: filename (with spaces), then type char, then size, then date
            # Example: "file.xlsx       A    19489  Thu Apr 10 14:41:14 2025"
            match = re.match(
                r"^(.+?)\s+([A-Z])\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)\s+(\d+:\d+:\d+)\s+(\d+)$",
                stripped,
            )
            if match:
                name = match.group(1).strip()
                type_char = match.group(2)
                size = int(match.group(3))  # File size in bytes
                # day_name = match.group(4)  # Not used
                month_name = match.group(5)
                day = match.group(6)
                time_str = match.group(7)
                year = match.group(8)

                # Parse the date
                try:
                    date_string = f"{month_name} {day} {year} {time_str}"
                    date_modified = datetime.strptime(date_string, "%b %d %Y %H:%M:%S")
                except ValueError:
                    date_modified = None

                # Skip . and .. entries
                if name in (".", ".."):
                    i += 1
                    continue

                # Determine item type
                try:
                    item_type = SMBItemType(type_char)
                except ValueError:
                    # Unknown type, default to file
                    item_type = SMBItemType.FILE

                # Build the full path for this item
                item_path = current_path + "/" + name if current_path else name
                # Prepend base_directory if provided to ensure all paths are
                # relative to share root
                full_path = (
                    f"{base_directory}/{item_path}".lstrip("/")
                    if base_directory
                    else item_path
                )

                # Create the item with the full path, date, and size
                item = SMBItem(name, item_type, full_path, date_modified, size)

                # Add to current parent (only if we're in a subdirectory context)
                if current_parent:
                    current_parent.children.append(item)
                else:
                    # If no current parent, this is a root-level item
                    # Check if it already exists in root_items
                    existing = None
                    for root_item in root_items:
                        if root_item.name == name and root_item.type == item_type:
                            existing = root_item
                            break

                    if not existing:
                        root_items.append(item)
                        # Don't set current_parent for root items - current_parent
                        # is only for subdirectories
            # If regex doesn't match, skip the line (could be malformed)

        i += 1

    return root_items


class SMBClientWrapper(Source):
    """Lightweight `Source` connector built around helpers in this module.

    This class does **not** introduce new complex logic (such as full tree scanning),
    it only wraps the existing helper functions:

    - `get_hybrid_listing_with_fallback` - directory listing,
    - `download_file_from_smb_with_retry` - downloading a file to local disk,
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
        """Initialize the SMB wrapper based on the `smbclient` CLI.

        Args:
            server (str): SMB server host name or IP.
            share (str): SMB share name on the server.
            smb_credentials (SMBCredentials | None): SMB credentials (same structure
                as for the `SMB` source). If None, they can be loaded from config.
            config_key (str | None): Optional key in viadot config. If provided and
                `smb_credentials` is None, SMB creds will be read from config
                (`username` and `password` fields).
        """
        raw_creds_from_config: dict[str, Any] = {}
        if smb_credentials is None and config_key:
            raw_creds_from_config = get_source_credentials(config_key) or {}
            smb_from_config = {
                "username": raw_creds_from_config.get("username"),
                "password": raw_creds_from_config.get("password"),
            }
            smb_credentials = SMBCredentials(**smb_from_config)

        if smb_credentials is None:
            msg = "`smb_credentials` must be provided either directly or via `config_key`."
            raise CredentialError(msg)

        # Cache credentials on the instance (same model as in `SMB` source)
        self._smb_credentials: SMBCredentials = smb_credentials

        # Pass validated credentials dict to the base `Source`, mirroring `SMB`
        super().__init__(
            *args, credentials=self._smb_credentials.model_dump(), **kwargs
        )

        self.server = server
        self.share = share

    @property
    def smb_username(self) -> str:
        """SMB username."""
        return self._smb_credentials.username

    @property
    def smb_password(self) -> str:
        """SMB password value."""
        # `SMBCredentials.password` is a `SecretStr` in the `SMB` source.
        return self._smb_credentials.password.get_secret_value()

    def list_directory(
        self,
        directory: str = "",
        recursive_timeout: int = 300,
        skip_root_recursive: bool = False,
    ) -> list[SMBItem]:
        """Return directory structure as a list of `SMBItem`.

        Args:
            directory (str): Start directory relative to the share root
                (e.g. `""` or `"data/reports"`).
            recursive_timeout (int): Timeout for recursive listing attempts.
            skip_root_recursive (bool): If True, skip the initial recursive
                listing on root.
        """
        return get_hybrid_listing_with_fallback(
            server=self.server,
            share=self.share,
            start_directory=directory,
            username=self.smb_username,
            password=self.smb_password,
            recursive_timeout=recursive_timeout,
            skip_root_recursive=skip_root_recursive,
        )

    def download_file(
        self,
        remote_path: str,
        local_path: str = "/tmp/smb_files",  # noqa: S108
        timeout: int = 900,
        max_retries: int = 3,
        base_delay: float = 2.0,
        prefix_levels_to_add: int = 0,
    ) -> str | None:
        """Download a single file from SMB to local disk.

        Args:
            remote_path (str): Path to the file relative to the share root.
            local_path (str): Local directory where the file should be saved.
            timeout (int): SMB operation timeout in seconds.
            max_retries (int): Maximum number of retry attempts on failure.
            base_delay (float): Base delay (seconds) used for exponential
                backoff.
            prefix_levels_to_add (int): Number of parent directory levels to
                prepend as a prefix to the local filename.
        """
        return download_file_from_smb_with_retry(
            server=self.server,
            share=self.share,
            remote_path=remote_path,
            username=self.smb_username,
            password=self.smb_password,
            local_path=local_path,
            timeout=timeout,
            max_retries=max_retries,
            base_delay=base_delay,
            prefix_levels_to_add=prefix_levels_to_add,
        )

    def stream_files_to_s3(
        self,
        smb_file_paths: list[str],
        s3_path: str,
        aws_credentials: dict[str, Any] | None = None,
        timeout: int = 900,
        max_retries: int = 3,
        base_delay: float = 2.0,
        prefix_levels_to_add: int = 0,
    ) -> list[str]:
        """Stream multiple files from SMB directly to S3.

        Args:
            smb_file_paths (list[str]): List of file paths relative to the
                share root.
            s3_path (str): Base S3 path (e.g. ``s3://bucket/folder/``).
            aws_credentials (dict[str, Any] | None): Optional AWS credential
                mapping used by the AWS CLI.
            timeout (int): Timeout for a single streaming operation.
            max_retries (int): Maximum number of retry attempts on failure.
            base_delay (float): Base delay (seconds) used for exponential
                backoff.
            prefix_levels_to_add (int): Number of parent directory levels to
                prepend as a prefix to the S3 object name.
        """
        return stream_smb_files_to_s3(
            smb_file_paths=smb_file_paths,
            smb_server=self.server,
            smb_share=self.share,
            s3_path=s3_path,
            smb_username=self.smb_username,
            smb_password=self.smb_password,
            aws_credentials=aws_credentials,
            timeout=timeout,
            max_retries=max_retries,
            base_delay=base_delay,
            prefix_levels_to_add=prefix_levels_to_add,
        )
