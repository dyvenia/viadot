"""SMB (file-sharing protocol) connector."""

from pathlib import Path
import re
import zipfile

import pendulum
from pydantic import BaseModel, SecretStr, root_validator
import smbclient
import smbprotocol

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import parse_dates


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
        base_paths: list[str],
        credentials: SMBCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the SMB with a base path.

        Args:
            base_paths (list[str]): The root directory or directories to start scanning
                from.
            credentials (SMBCredentials): Sharepoint credentials.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials.
        """
        self.base_paths = base_paths

        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = SMBCredentials(**raw_creds)
        super().__init__(*args, credentials=validated_creds.model_dump(), **kwargs)

        normalized_paths = [re.sub(r"\\+", r"\\", path) for path in self.base_paths]

        parts = normalized_paths[0].lstrip("\\").split("\\")
        if not parts or not parts[0]:
            msg = f"Invalid base path format: {self.base_paths[0]}"
            raise ValueError(msg)

        server_host_or_ip = parts[0]

        try:
            smbclient.register_session(
                server_host_or_ip,
                username=self.credentials.get("username"),
                password=self.credentials.get("password").get_secret_value(),
            )
            self.logger.info("Connection succesfully established.")
        except smbprotocol.exceptions.LogonFailure:
            self.logger.exception("Authentication failed: invalid credentials.")
            raise
        except smbprotocol.exceptions.PasswordExpired:
            self.logger.exception("Authentication failed: password expired.")
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
        zip_inner_file_regexes: str | list[str] | None = None,
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
            zip_inner_file_regexes (str | list[str] | None): Regular expression string
                or list of regex patterns used to filter files *inside* ZIP archives.
                If provided, ZIP files will be unpacked and only matching inner files
                will be extracted and stored. Defaults to None.

        Returns:
            tuple[dict[str, bytes], list[str]]:
            - A dictionary mapping file paths to their contents in bytes.
            - A list of file paths that were skipped or failed to be read.
        """
        date_filter_parsed = parse_dates(
            date_filter=date_filter,
            dynamic_date_symbols=dynamic_date_symbols,
            dynamic_date_format=dynamic_date_format,
            dynamic_date_timezone=dynamic_date_timezone,
        )

        return self._scan_directories(
            paths=self.base_paths,
            filename_regex=filename_regex,
            extensions=extensions,
            date_filter_parsed=date_filter_parsed,
            prefix_levels_to_add=prefix_levels_to_add,
            zip_inner_file_regexes=zip_inner_file_regexes,
        )

    def _scan_directories(
        self,
        paths: list[str],
        filename_regex: str | list[str] | None = None,
        extensions: str | list[str] | None = None,
        date_filter_parsed: pendulum.Date
        | tuple[pendulum.Date, pendulum.Date]
        | None = None,
        prefix_levels_to_add: int = 0,
        zip_inner_file_regexes: str | list[str] | None = None,
    ) -> tuple[dict[str, bytes], list[str]]:
        """Recursively scans a directory for matching files based on filters.

        It applies 'filename_regex' and 'extensions' filters and can filter files based
        on modification dates - 'date_filter_parsed'.

        It applies filters to both files and directories. Directories are only scanned
        recursively if their modification date matches the given filter.
        This optimization avoids unnecessary traversal of unchanged folders.

        Args:
            paths (list[str]): The root directory or directories to start scanning from.
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
            zip_inner_file_regexes (str | list[str] | None): Regular expression string
                or list of regex patterns used to filter files *inside* ZIP archives.
                If provided, ZIP files will be unpacked and only matching inner files
                will be extracted and stored. Defaults to None.

        Returns:
            tuple[dict[str, bytes], list[str]]:
            - A dictionary mapping file paths to their contents in bytes.
            - A list of file paths that were skipped or failed to be read.
        """
        found_files = {}
        problematic_entries = []

        for path in paths:
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
                            self._get_file_content(
                                entry, prefix_levels_to_add, zip_inner_file_regexes
                            )
                        )

                    elif entry.is_dir():
                        date_match = self._is_date_match(
                            entry_mod_date_parsed, date_filter_parsed
                        )

                        if date_match:
                            found_files.update(
                                self._scan_directories(
                                    paths=[entry.path],
                                    filename_regex=filename_regex,
                                    extensions=extensions,
                                    date_filter_parsed=date_filter_parsed,
                                    prefix_levels_to_add=prefix_levels_to_add,
                                    zip_inner_file_regexes=zip_inner_file_regexes,
                                )[0]  # Only the matched files dict is used
                            )
                except smbprotocol.exceptions.SMBOSError as e:
                    self.logger.warning(f"Entry not found: {e}")
                    problematic_entries.append(entry.name)
                except Exception:
                    self.logger.exception(f"Error scanning or downloading from {path}.")
                    raise

        return found_files, problematic_entries

    def _build_prefix_from_path(self, file_path: str, levels: int) -> str:
        """Extract parent folder names to use as prefix.

        Args:
            file_path (str): The full or relative path to the file.
            levels (int): Number of parent folder levels to include.

        Returns:
            str: Prefix string with parent folder names joined by underscores.
                 Returns empty string if levels is 0 or no valid parents exist.
        """
        if levels <= 0:
            return ""

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

    def _add_prefix_to_filename(self, filename: str, prefix: str) -> str:
        """Combine prefix and filename with underscore separator.

        Args:
            filename (str): The base filename.
            prefix (str): The prefix to add.

        Returns:
            str: Prefixed filename, or original filename if prefix is empty.
        """
        if not prefix:
            return filename
        return f"{prefix}_{filename}"

    def _get_file_content(
        self,
        entry: smbclient._os.SMBDirEntry,
        prefix_levels_to_add: int = 0,
        zip_inner_file_regexes: str | list[str] | None = None,
    ) -> dict[str, bytes]:
        """Extracts the content of a file from an SMB directory entry.

        For ZIP files, extracts files matching the zip_inner_file_regexes pattern.
        For other files, reads the entire content.

        Args:
            entry (smbclient._os.SMBDirEntry): An SMB directory entry object.
            prefix_levels_to_add (int): Number of parent folder levels to include
                as prefix.
            zip_inner_file_regexes (str | list[str] | None): Regular expression string
                or list of regex patterns used to filter files *inside* ZIP archives.
                If provided, ZIP files will be unpacked and only matching inner files
                will be extracted and stored. Defaults to None.

        Returns:
            dict[str, bytes]: Dictionary with file name(s) as keys and content
                as values.
        """
        file_path = entry.path
        contents = {}

        self.logger.info(f"Found: {file_path}")

        # Build prefix once for this file
        prefix = self._build_prefix_from_path(file_path, prefix_levels_to_add)
        is_zip = file_path.lower().endswith(".zip")

        if is_zip and zip_inner_file_regexes:
            try:
                with smbclient.open_file(file_path, mode="rb") as remote_zip:  # noqa: SIM117
                    with zipfile.ZipFile(remote_zip) as zf:
                        matched_count = 0
                        for zip_member_name in zf.namelist():
                            # Skip directories inside ZIP
                            if zip_member_name.endswith("/"):
                                continue

                            if self._matches_any_regex(
                                text=zip_member_name,
                                patterns=zip_inner_file_regexes,
                            ):
                                with zf.open(zip_member_name) as member:
                                    content = member.read()
                                    # Use only the base filename from ZIP member
                                    base_name = Path(zip_member_name).name
                                    prefixed_name = self._add_prefix_to_filename(
                                        base_name, prefix
                                    )
                                    contents[prefixed_name] = content
                                    matched_count += 1

                        if matched_count == 0:
                            self.logger.debug(
                                f"No matching files found in ZIP: {file_path}. "
                                f"Filter: {zip_inner_file_regexes}"
                            )

            except zipfile.BadZipFile:
                self.logger.warning(f"Invalid ZIP file: {file_path}")
            except Exception as e:
                self.logger.exception(f"Error reading ZIP file {file_path}: {e}")  # noqa: TRY401
                raise
        else:
            # Regular file handling
            with smbclient.open_file(file_path, mode="rb") as file:
                content = file.read()

            file_path_normalized = file_path.replace("\\", "/")
            filename = Path(file_path_normalized).name
            prefixed_name = self._add_prefix_to_filename(filename, prefix)
            contents[prefixed_name] = content

        return contents

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
