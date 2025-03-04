"""SMB (file-sharing protocol) connector."""

from pathlib import Path

import pendulum
from pydantic import BaseModel, root_validator
import smbclient

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.orchestration.prefect.utils import DynamicDateHandler
from viadot.sources.base import Source


class SMBCredentials(BaseModel):
    username: str  # username (e.g username@tenant_name.com)
    password: str

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
            msg = "'username', and 'password' credentials are required."
            raise CredentialError(msg)
        return credentials


class SMB(Source):
    def __init__(
        self,
        base_path: str,
        credentials: SMBCredentials = None,
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
        self.found_files = {}
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(SMBCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        smbclient.ClientConfig(
            username=self.credentials.get("username"),
            password=self.credentials.get("password"),
        )

    def scan_and_store(
        self,
        keywords: list[str] | None = None,
        extensions: list[str] | None = None,
        date_filter: str | tuple[str, str] | None = None,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y-%m-%d",
        dynamic_date_timezone: str = "Europe/Warsaw",
    ) -> dict[str, bytes]:
        """Scan the directory structure for files and store their contents in memory.

        Args:
            keywords (list[str] | None): List of keywords to search for in filenames.
            extensions (list[str] | None): List of file extensions to filter by.
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
                processing. Defaults to "Europe/Warsaw".

        Returns:
            dict[str, bytes]: A dictionary mapping file paths to their contents.
        """
        date_filter_parsed = self._parse_dates(
            date_filter=date_filter,
            dynamic_date_symbols=dynamic_date_symbols,
            dynamic_date_format=dynamic_date_format,
            dynamic_date_timezone=dynamic_date_timezone,
        )

        self._scan_directory(self.base_path, keywords, extensions, date_filter_parsed)
        return self.found_files

    def _parse_dates(
        self,
        date_filter: str | tuple[str, str] | None = None,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y-%m-%d",
        dynamic_date_timezone: str = "Europe/Warsaw",
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
                Defaults to "Europe/Warsaw".

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
        keywords: list[str] | None,
        extensions: list[str] | None,
        date_filter_parsed: pendulum.Date
        | tuple[pendulum.Date, pendulum.Date]
        | None = None,
    ) -> None:
        """Recursively scans a directory for matching files based on filters.

        It applies keyword and extension filters and can filter files based on
        modification dates.

        Args:
            path (str): The directory path to scan.
            keywords (list[str] | None): List of keywords to search for in filenames.
                Defaults to None.
            extensions (list[str] | None): List of file extensions to filter by.
                Defaults to None.
            date_filter_parsed (
                pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None
            ):
                - A single `pendulum.Date` for exact date filtering.
                - A tuple of two `pendulum.Date` values for date range filtering.
                - None, if no date filter is applied.
                Defaults to None.
        """
        try:
            entries = self._get_directory_entries(path)
            for entry in entries:
                self._handle_directory_entry(
                    entry, path, keywords, extensions, date_filter_parsed
                )
        except Exception as e:
            self.logger.exception(f"Error scanning or downloading from {path}: {e}")  # noqa: TRY401

    def _get_directory_entries(self, path: str):
        """Get directory entries using smbclient.

        Args:
            path (str): The directory path to scan.

        Returns:
            Iterator: An iterator of directory entries.
        """
        return smbclient.scandir(path)

    def _handle_directory_entry(
        self,
        entry: smbclient._os.SMBDirEntry,
        parent_path: str,
        keywords: list[str] | None = None,
        extensions: list[str] | None = None,
        date_filter_parsed: pendulum.Date
        | tuple[pendulum.Date, pendulum.Date]
        | None = None,
    ):
        """Process a single directory entry.

        It processes either by recursing into subdirectories or handling matching files.

        Args:
            entry (smbclient._os.SMBDirEntry): A directory entry object from
                the directory scan.
            parent_path (str): The parent directory path.
            keywords (list[str] | None): List of keywords to search for in filenames.
                Defaults to None.
            extensions (list[str] | None): List of file extensions to filter by.
                Defaults to None.
            date_filter_parsed (
                pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None
            ):
                - A single `pendulum.Date` for exact date filtering.
                - A tuple of two `pendulum.Date` values for date range filtering.
                - None, if no date filter is applied.
                Defaults to None.
        """
        full_path = Path(parent_path) / entry.name
        if entry.is_dir():
            self._scan_directory(full_path, keywords, extensions, date_filter_parsed)
        elif self._is_matching_file(entry, keywords, extensions, date_filter_parsed):
            self._store_matching_file(file_path=full_path)

    def _is_matching_file(
        self,
        entry: smbclient._os.SMBDirEntry,
        keywords: list[str] | None = None,
        extensions: list[str] | None = None,
        date_filter_parsed: pendulum.Date
        | tuple[pendulum.Date, pendulum.Date]
        | None = None,
    ) -> bool:
        """Check if a file matches the given criteria.

        It verifies whether the file satisfies any combination of:
        - Keyword-based filtering.
        - Extension-based filtering.
        - Exact date or date range filtering.

        Args:
            entry (smbclient._os.SMBDirEntry): A directory entry object from
                the directory scan.
            keywords (list[str] | None): List of keywords to search for in filenames.
                It is case-insensitive. Defaults to None.
            extensions (list[str] | None): List of file extensions to filter by.
                It is case-insensitive. Defaults to None.
            date_filter_parsed (
                pendulum.Date | tuple[pendulum.Date, pendulum.Date] | None
            ):
                - A single `pendulum.Date` for exact date filtering.
                - A tuple of two `pendulum.Date` values for date range filtering.
                - None, if no date filter is applied.
                Defaults to None.

        Returns:
            bool: True if the file matches all criteria, False otherwise.
        """
        if not entry.is_file():
            return False

        name_lower = entry.name.lower()
        file_creation_time = pendulum.instance(entry._dir_info.creation_time)
        file_creation_date = file_creation_time.date()

        matches_extension = not extensions or any(
            name_lower.endswith(ext.lower()) for ext in extensions
        )
        matches_keyword = not keywords or any(
            keyword.lower() in name_lower for keyword in keywords
        )

        matches_date = True  # Default to True if no date filtering is applied
        if isinstance(date_filter_parsed, pendulum.Date):
            matches_date = file_creation_date == date_filter_parsed
        elif isinstance(date_filter_parsed, tuple):
            start_date, end_date = date_filter_parsed
            matches_date = start_date <= file_creation_date <= end_date

        return matches_extension and matches_keyword and matches_date

    def _store_matching_file(self, file_path: str) -> None:
        """Process a matching file by fetching its content.

        It fetching the content and storing it in the found_files dictionary.

        Args:
            file_path (str): The full path of the matching file.
        """
        self.logger.info(f"Found: {file_path}")
        content = self._fetch_file_content(file_path)
        self.found_files[file_path] = content

    def _fetch_file_content(self, file_path: str) -> bytes:
        """Fetch the content of a file.

        Args:
            file_path (str): The full path of the file to download.

        Returns:
            bytes: The content of the file.
        """
        with smbclient.open_file(file_path, mode="rb") as file:
            return file.read()

    def save_stored_files(self, destination_dir: str) -> None:
        """Save stored files from memory to a local directory.

        Args:
            destination_dir (str): The local directory where files should be saved.
        """
        if not self.found_files:
            self.logger.info("No files to save.")
            return

        Path(destination_dir).mkdir(
            parents=True, exist_ok=True
        )  # Ensure the directory exists

        for file_path, content in self.found_files.items():
            local_filename = Path(destination_dir) / Path(file_path).name

            try:
                with Path(local_filename).open("wb") as f:
                    f.write(content)
                self.logger.info(f"Saved: {local_filename}")
            except Exception as e:
                self.logger.exception(f"Failed to save {local_filename}: {e}")  # noqa: TRY401
