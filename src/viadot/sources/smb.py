"""SMB (file-sharing protocol) connector."""

from pathlib import Path
import re

import pendulum
from pydantic import BaseModel, SecretStr, root_validator
import smbclient
import smbprotocol

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.orchestration.prefect.utils import DynamicDateHandler
from viadot.sources.base import Source


class SMBCredentials(BaseModel):
    username: str  # username (e.g username@tenant_name.com)
    password: SecretStr

    @root_validator(pre=True)
    def is_configured(cls, credentials: dict) -> dict:  # noqa: N805
        """Validate that both username and password are provided.

        This method is a Pydantic root validator that checks if both
        username and password fields are present and non-empty.

        Args:
            credentials (dict): A dictionary containing the credential fields.

        Returns:
            dict: The validated credentials dictionary.

        Raises:
            CredentialError: If either username or password is missing.
        """
        username = credentials.get("username")
        password = credentials.get("password")

        if not (username and password):
            msg = "`username`, and `password` credentials are required."
            raise CredentialError(msg)
        return credentials


class SMB(Source):
    def __init__(
        self,
        base_path: str,
        credentials: SMBCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the SMB with a base path.

        Args:
            base_path (str): The root directory to start scanning from.
            credentials (SMBCredentials): Sharepoint credentials.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials.
        """
        self.base_path = base_path
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = SMBCredentials(**raw_creds)
        super().__init__(*args, credentials=validated_creds.dict(), **kwargs)

        normalized_path = re.sub(r"\\+", r"\\", self.base_path)
        parts = normalized_path.lstrip("\\").split("\\")
        server_host_or_ip = parts[0]

        try:
            smbclient.register_session(
                server_host_or_ip,
                username=self.credentials.get("username"),
                password=self.credentials.get("password").get_secret_value(),
            )
            self.logger.info("Connection succesfully established.")
        except smbprotocol.exceptions.LogonFailure:
            self.logger.exception("Authentication failed: credentials invalid.")
            raise
        except smbprotocol.exceptions.PasswordExpired:
            self.logger.exception("Authentication failed: credentials expired.")
            raise
        except Exception:
            self.logger.exception("Connection failed.")
            raise

    def scan_and_store(
        self,
        filename_regex: str | list[str] | None = None,
        extensions: str | list[str] | None = None,
        date_filter: str | tuple[str, str] | None = None,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y-%m-%d",
        dynamic_date_timezone: str = "UTC",
        prefix_levels_to_add: int = 0,
    ) -> tuple[dict[str, bytes], list[str]]:
        """Scan the directory structure for files and store their contents in memory.

        Args:
            filename_regex (str | list[str] | None, optional): A regular expression
                string or list of regex patterns used to filter file names. If provided,
                only file names matching the pattern(s) will be included.
                Defaults to None.
            extensions (str | list[str] | None): List of file extensions or single
                string to filter by. Defaults to None.
            date_filter (str | tuple[str, str] | None):
                - A single date string (e.g., "2024-03-03").
                - A tuple containing exactly two date strings
                (e.g., ("2024-03-03", "2025-04-04")).
                - None, which returns None.
                Defaults to None.
            dynamic_date_symbols (list[str], optional): Symbols for dynamic date
                handling. Defaults to ["<<", ">>"].
            dynamic_date_format (str, optional): Format used for dynamic date parsing.
                Defaults to "%Y-%m-%d".
            dynamic_date_timezone (str, optional): Timezone used for dynamic date
                processing. Defaults to "UTC".
            prefix_levels_to_add (int, optional): Number of parent folder levels to
                include as a prefix to the filename,counting from the deepest (closest)
                folder upwards. Defaults to 0, meaning no prefix is added.

        Returns:
            tuple[dict[str, bytes], list[str]]:
            - A dictionary mapping file paths to their contents in bytes.
            - A list of file paths that were skipped or failed to be read.
        """
        date_filter_parsed = self._parse_dates(
            date_filter=date_filter,
            dynamic_date_symbols=dynamic_date_symbols,
            dynamic_date_format=dynamic_date_format,
            dynamic_date_timezone=dynamic_date_timezone,
        )

        return self._scan_directory(
            path=self.base_path,
            filename_regex=filename_regex,
            extensions=extensions,
            date_filter_parsed=date_filter_parsed,
            prefix_levels_to_add=prefix_levels_to_add,
        )

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

    def _scan_directory(
        self,
        path: str,
        filename_regex: str | list[str] | None = None,
        extensions: str | list[str] | None = None,
        date_filter_parsed: pendulum.Date
        | tuple[pendulum.Date, pendulum.Date]
        | None = None,
        prefix_levels_to_add: int = 0,
    ) -> tuple[dict[str, bytes], list[str]]:
        """Recursively scans a directory for matching files based on filters.

        It applies 'filename_regex' and 'extensions' filters and can filter files based
        on modification dates - 'date_filter_parsed'.

        It applies filters to both files and directories. Directories are only scanned
        recursively if their modification date matches the given filter.
        This optimization avoids unnecessary traversal of unchanged folders.

        Args:
            path (str): The directory path to scan.
            filename_regex (str | list[str] | None, optional): A regular expression
                string or list of regex patterns used to filter file names. If provided,
                only file names matching the pattern(s) will be included.
                Defaults to None.
            extensions (str | list[str] | None): List of file extensions or single
                string to filter by. Defaults to None.
            date_filter_parsed (
                pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None
            ):
                - A single `pendulum.Date` for exact date filtering.
                - A tuple of two `pendulum.Date` values for date range filtering.
                - None, if no date filter is applied.
                Defaults to None.
            prefix_levels_to_add (int, optional): Number of parent folder levels to
                include as a prefix to the filename,counting from the deepest (closest)
                folder upwards. Defaults to 0, meaning no prefix is added.

        Returns:
            tuple[dict[str, bytes], list[str]]:
            - A dictionary mapping file paths to their contents in bytes.
            - A list of file paths that were skipped or failed to be read.
        """
        found_files = {}
        problematic_entries = []

        entries = self._get_directory_entries(path)
        for entry in entries:
            # Skip temp files
            if entry.name.startswith("~$"):
                problematic_entries.append(entry.name)
                continue

            try:
                entry_mod_date_parsed = pendulum.from_timestamp(
                    entry.stat().st_mtime
                ).date()
                entry_name = entry.name

                if entry.is_file() and self._is_matching_file(
                    file_name=entry_name,
                    file_mod_date_parsed=entry_mod_date_parsed,
                    filename_regex=filename_regex,
                    extensions=extensions,
                    date_filter_parsed=date_filter_parsed,
                ):
                    found_files.update(
                        self._get_file_content(entry, prefix_levels_to_add)
                    )

                elif entry.is_dir():
                    date_match = self._is_date_match(
                        entry_mod_date_parsed, date_filter_parsed
                    )

                    if date_match:
                        found_files.update(
                            self._scan_directory(
                                entry.path,
                                filename_regex,
                                extensions,
                                date_filter_parsed,
                                prefix_levels_to_add,
                            )[0]  # Only the matched files dict is used
                        )
            except smbprotocol.exceptions.SMBOSError as e:
                self.logger.warning(f"Entry not found: {e}")
                problematic_entries.append(entry.name)
            except Exception:
                self.logger.exception(f"Error scanning or downloading from {path}.")
                raise

        return found_files, problematic_entries

    def _add_prefix_to_file_name(
        self, file_path: str, prefix_levels_to_add: int = 0
    ) -> str:
        """Generate a new filename by adding parent folder names as prefix.

        Args:
            file_path (str): The full or relative path to the file.
            prefix_levels_to_add (int, optional): Number of parent folder levels to
                include as a prefix to the filename,counting from the deepest (closest)
                folder upwards. Defaults to 0, meaning no prefix is added.

        Returns:
            str: New filename with the specified parent folders as prefix separated by
                underscores.If no prefix levels are specified, returns the original
                filename.
        """
        normalized_path = file_path.replace("\\", "/")
        path = Path(normalized_path)

        parent_parts = [
            p for p in path.parent.parts if p and p not in ("/", "\\", ".", "//", "")
        ]

        # Normalize prefix level count to valid range
        levels = max(0, prefix_levels_to_add)
        levels = min(levels, len(parent_parts))

        prefix_parts_to_add = parent_parts[-levels:] if levels > 0 else []

        prefix_str = "_".join(prefix_parts_to_add)

        return "_".join([prefix_str, path.name]) if prefix_str else path.name

    def _get_file_content(
        self, entry: smbclient._os.SMBDirEntry, prefix_levels_to_add: int = 0
    ) -> dict[str, bytes]:
        """Extracts the content of a file from an SMB directory entry.

        This function takes an SMB directory entry, logs the file path,
        fetches the file's content, and returns a dictionary with the
        file name as the key and its content as the value.

        Args:
            entry (smbclient._os.SMBDirEntry): An SMB directory entry object.
            prefix_levels_to_add (int, optional): Number of parent folder levels to
                include as a prefix to the filename,counting from the deepest (closest)
                folder upwards. Defaults to 0, meaning no prefix is added.

        Returns:
            dict[str, bytes]: A dictionary with a single key-value pair, where the key
                is the file name and the value is the file's content.
        """
        file_path = entry.path

        self.logger.info(f"Found: {file_path}")

        with smbclient.open_file(file_path, mode="rb") as file:
            content = file.read()

        file_name = self._add_prefix_to_file_name(
            file_path=file_path, prefix_levels_to_add=prefix_levels_to_add
        )

        return {file_name: content}

    def _get_directory_entries(self, path: str):
        """Get directory entries using smbclient.

        Args:
            path (str): The directory path to scan.

        Returns:
            Iterator: An iterator of directory entries.
        """
        return smbclient.scandir(path)

    def _is_date_match(
        self,
        file_modification_date: pendulum.Date,
        date_filter_parsed: pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None,
    ) -> bool:
        """Check if the file modification date matches the given date filter.

        Args:
            file_modification_date (pendulum.Date): The modification date of the file.
            date_filter_parsed (
                pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None
            ):
                - A single `pendulum.Date` for exact date filtering.
                - A tuple of two `pendulum.Date` values for date range filtering.
                - None, if no date filter is applied. Defaults to None.

        Returns:
            bool: True if the file_modification_date matches the filter or if no filter
                is applied. False otherwise.
        """
        if date_filter_parsed is None:
            return True

        if isinstance(date_filter_parsed, pendulum.Date):
            return file_modification_date == date_filter_parsed

        if isinstance(date_filter_parsed, tuple):
            start_date, end_date = date_filter_parsed
            return start_date <= file_modification_date <= end_date

        return False

    def _is_matching_file(
        self,
        file_name: str,
        file_mod_date_parsed: pendulum.Date,
        filename_regex: str | list[str] | None = None,
        extensions: str | list[str] | None = None,
        date_filter_parsed: pendulum.Date
        | tuple[pendulum.Date, pendulum.Date]
        | None = None,
    ) -> bool:
        """Check if a file matches the given criteria.

        It verifies whether the file satisfies any combination of:
        - Filename regular expression filtering.
        - Extension-based filtering.
        - Exact date or date range filtering.

        Args:
            file_name (str): The name of the file to evaluate.
            file_mod_date_parsed (pendulum.Date): The parsed modification date of
                the file.
            filename_regex (str | list[str] | None, optional): A regular expression
                string or list of regex patterns used to filter file names. If provided,
                only file names matching the pattern(s) will be included.
                Defaults to None.
            extensions (str | list[str] | None): List of file extensions or single
                string to filter by. It is case-insensitive. Defaults to None.
            date_filter_parsed (
                pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None
            ):
                - A single `pendulum.Date` for exact date filtering.
                - A tuple of two `pendulum.Date` values for date range filtering.
                - None, if no date filter is applied. Defaults to None.

        Returns:
            bool: True if the file matches all criteria or no criteria are provided,
                False otherwise.
        """
        name_lower = file_name.lower()

        # Normalize to lists
        filename_regex_list = (
            [filename_regex] if isinstance(filename_regex, str) else filename_regex
        )
        extension_list = [extensions] if isinstance(extensions, str) else extensions

        matches_extension = not extension_list or any(
            isinstance(ext, str) and name_lower.endswith(ext.lower())
            for ext in extension_list
        )

        matches_filename = (
            True
            if not filename_regex_list
            else any(
                self._safe_regex_match(pattern, name_lower)
                for pattern in filename_regex_list
            )
        )

        if not matches_extension or not matches_filename:
            return False

        if date_filter_parsed:
            return self._is_date_match(file_mod_date_parsed, date_filter_parsed)

        return True

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
            self.logger.warning(f"Invalid regex pattern: {pattern} — Error: {e}")
            return False

    def save_files_locally(
        self, file_data: dict[str, bytes], destination_dir: str
    ) -> None:
        """Save stored files from memory to a local directory.

        This function takes a dictionary of file names and their contents,
        and saves each file to the specified destination directory. It creates
        the destination directory if it doesn't exist.

        Args:
        file_data (dict[str, bytes]): A dictionary where keys are file names and values
            are file contents as bytes.
        destination_dir (str): The local directory where files should be saved.

        Raises:
            Exception: If there's an error while saving a file, it's logged but
                not raised.

        Note:
        - If files_to_store is empty, the function logs a message and returns.
        - Each file saving operation is wrapped in a try-except block to handle
        potential errors individually.
        """
        if not file_data:
            self.logger.info("No files to save.")
            return

        Path(destination_dir).mkdir(
            parents=True, exist_ok=True
        )  # Ensure the directory exists

        for file_path, content in file_data.items():
            local_filename = Path(destination_dir) / Path(file_path).name
            try:
                with Path(local_filename).open("wb") as f:
                    f.write(content)
                self.logger.info(f"Saved: {local_filename}")
            except Exception as e:
                self.logger.exception(f"Failed to save {local_filename}: {e}")  # noqa: TRY401
