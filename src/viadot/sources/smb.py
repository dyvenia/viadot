import os

from pydantic import BaseModel
import smbclient

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source


class SMBCredentials(BaseModel):
    username: str  # username (e.g username@{tenant_name}.com)
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
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(SMBCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        smbclient.ClientConfig(
            username=self.credentials.get("username"),
            password=self.credentials.get("password"),
        )

    def scan_and_download(
        self,
        keywords: list[str] | None = None,
        extensions: list[str] | None = None,
    ) -> dict[str, bytes]:
        """Scan the directory structure for files and download their contents.

        Args:
            keywords (list[str] | None): List of keywords to search for in filenames.
                Defaults to None.
            extensions (list[str] | None): List of file extensions to filter by.
                Defaults to None.

        Returns:
            Dict[str, bytes]: A dictionary mapping file paths to their contents.
        """
        self.found_files = {}

        self._scan_directory(self.base_path, keywords, extensions)
        return self.found_files

    def _scan_directory(
        self,
        path: str,
        keywords: list[str] | None,
        extensions: list[str] | None,
    ):
        """Recursively scan a directory for matching files.

        Args:
            path (str): The current directory path to scan.
            keywords (list[str] | None): List of keywords to search for in filenames.
            extensions (list[str] | None): List of file extensions to filter by.
        """
        try:
            entries = self._get_directory_entries(path)
            for entry in entries:
                self._process_entry(entry, path, keywords, extensions)
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

    def _process_entry(
        self,
        entry: smbclient._os.SMBDirEntry,
        parent_path: str,
        keywords: list[str] | None = None,
        extensions: list[str] | None = None,
    ):
        """Process a single directory entry.

        It processes either by recursing into subdirectories or handling matching files.

        Args:
            entry: A directory entry object.
            parent_path (str): The parent directory path.
            keywords (list[str] | None): List of keywords to search for in filenames.
                Defaults to None.
            extensions (list[str] | None): List of file extensions to filter by.
                Defaults to None.
        """
        full_path = os.path.join(parent_path, entry.name)
        if entry.is_dir():
            self._scan_directory(full_path, keywords, extensions)
        elif self._is_matching_file(entry, keywords, extensions):
            self._process_matching_file(file_path=full_path)

    def _is_matching_file(
        self,
        entry: smbclient._os.SMBDirEntry,
        keywords: list[str] | None = None,
        extensions: list[str] | None = None,
    ) -> bool:
        """Check if a file matches the given criteria including keywords and extensions.

        Args:
            entry: A file entry object from the directory scan.
            keywords (list[str] | None): List of keywords to search for in filenames.
                It is case-insensitive. Defaults to None.
            extensions (list[str] | None): List of file extensions to filter by. It is
                case-insensitive. Defaults to None.

        Returns:
            bool: True if the file matches all criteria, False otherwise.
        """
        if not entry.is_file():
            return False

        name_lower = entry.name.lower()

        matches_extension = not extensions or any(
            name_lower.endswith(ext.lower()) for ext in extensions
        )

        matches_keyword = not keywords or any(
            keyword.lower() in name_lower for keyword in keywords
        )

        return matches_extension and matches_keyword

    def _process_matching_file(self, file_path: str) -> None:
        """Process a matching file by downloading its content.

        It downloading the content and storing it in the found_files dictionary.

        Args:
            file_path (str): The full path of the matching file.
        """
        self.logger.info(f"Found: {file_path}")
        content = self._download_file_content(file_path)
        self.found_files[file_path] = content

    def _download_file_content(self, file_path: str) -> bytes:
        """Download the content of a file.

        Args:
            file_path (str): The full path of the file to download.

        Returns:
            bytes: The content of the file.
        """
        with smbclient.open_file(file_path, mode="rb") as file:
            return file.read()
