"""Unit tests for smbclient_wrapper module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pendulum
from pydantic import SecretStr
import pytest

from viadot.exceptions import (
    CredentialError,
)
from viadot.sources.smb_client import (
    SMBClient,
    SMBItem,
    SMBItemType,
    _add_prefix_to_filename,
    _build_prefix_from_path,
    _check_filename_for_problematic_chars,
    _get_unique_local_path,
)


SERVER = "test-server"
SHARE = "test-share"
USERNAME = "test_user"
PASSWORD = "test_password"  # noqa: S105 # pragma: allowlist secret


@pytest.fixture
def valid_credentials():
    """Return credentials as dict (as expected by new __init__ implementation)."""
    return {
        "username": USERNAME,
        "password": SecretStr(PASSWORD),
    }


@pytest.fixture
def smb_client_instance(valid_credentials):
    """Create an SMBClient instance for testing."""
    return SMBClient(
        server=SERVER,
        share=SHARE,
        smb_credentials=valid_credentials,
    )


@pytest.fixture
def mock_smb_item_file():
    """Create a mock SMBItem representing a file."""
    return SMBItem(
        name="test_file.txt",
        item_type=SMBItemType.FILE,
        path="test_file.txt",
        date_modified=datetime(2025, 1, 15, 10, 30, 0),
        size=1024,
    )


@pytest.fixture
def mock_smb_item_dir():
    """Create a mock SMBItem representing a directory."""
    return SMBItem(
        name="test_folder",
        item_type=SMBItemType.DIRECTORY,
        path="test_folder",
        children=[],
    )


# ==================== SMBClient.__init__ tests ====================


def test_smb_client_initialization_with_credentials(valid_credentials):
    """Test SMBClient initialization with credentials."""
    client = SMBClient(
        server=SERVER,
        share=SHARE,
        smb_credentials=valid_credentials,
    )
    assert client.server == SERVER
    assert client.share == SHARE
    assert client.credentials["username"] == USERNAME
    assert client.credentials["password"].get_secret_value() == PASSWORD


def test_smb_client_initialization_with_config_key():
    """Test SMBClient initialization with config_key."""
    mock_creds = {
        "username": USERNAME,
        "password": PASSWORD,
    }
    with patch(
        "viadot.sources.smb_client.get_source_credentials",
        return_value=mock_creds,
    ):
        client = SMBClient(
            server=SERVER,
            share=SHARE,
            config_key="test_config_key",
        )
        assert client.server == SERVER
        assert client.share == SHARE
        assert client.credentials["username"] == USERNAME
        assert client.credentials["password"].get_secret_value() == PASSWORD


def test_smb_client_initialization_without_credentials():
    """Test SMBClient initialization fails without credentials."""
    with pytest.raises(
        CredentialError,
        match="`username`, and `password` credentials are required.",
    ):
        SMBClient(server=SERVER, share=SHARE)


# ==================== SMBClient.list_directory tests ====================


def test_list_directory_success(smb_client_instance):
    """Test successful directory listing."""
    mock_items = [
        SMBItem(
            name="file1.txt",
            item_type=SMBItemType.FILE,
            path="file1.txt",
            date_modified=datetime(2025, 1, 15),
            size=100,
        ),
        SMBItem(
            name="dir1",
            item_type=SMBItemType.DIRECTORY,
            path="dir1",
            children=[],
        ),
    ]

    with patch(
        "viadot.sources.smb_client.get_listing_with_shallow_first",
        return_value=mock_items,
    ) as mock_listing:
        result = smb_client_instance.list_directory(directory="test_dir")

        assert len(result) == 2
        assert result[0].name == "file1.txt"
        assert result[1].name == "dir1"
        mock_listing.assert_called_once()
        call_kwargs = mock_listing.call_args.kwargs
        assert call_kwargs["server"] == SERVER
        assert call_kwargs["share"] == SHARE
        assert call_kwargs["start_directory"] == "test_dir"


def test_list_directory_with_custom_timeout(smb_client_instance):
    """Test directory listing with custom recursive_timeout."""
    mock_items = [SMBItem(name="file.txt", item_type=SMBItemType.FILE, path="file.txt")]

    with patch(
        "viadot.sources.smb_client.get_listing_with_shallow_first",
        return_value=mock_items,
    ) as mock_listing:
        result = smb_client_instance.list_directory(directory="", recursive_timeout=600)

        assert len(result) == 1
        mock_listing.assert_called_once()
        call_kwargs = mock_listing.call_args.kwargs
        assert call_kwargs["recursive_timeout"] == 600


# ==================== UnstructuredSource method tests ====================


def test_download_to_local_success(smb_client_instance, tmp_path):
    """Test successful file download using download_to_local."""
    remote_path = "data/file.txt"
    local_path = str(tmp_path)
    expected_local_file = str(tmp_path / "file.txt")

    with patch(
        "viadot.sources.smb_client.download_file_from_smb",
        return_value=[expected_local_file],
    ) as mock_download:
        result = smb_client_instance.download_to_local(
            path=remote_path, to_path=local_path
        )

        assert result == [expected_local_file]
        mock_download.assert_called_once()
        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs["server"] == SERVER
        assert call_kwargs["share"] == SHARE
        assert call_kwargs["remote_path"] == remote_path
        assert call_kwargs["local_path"] == local_path


def test_download_to_local_with_kwargs(smb_client_instance, tmp_path):
    """Test download_to_local with additional kwargs like prefix_levels_to_add."""
    remote_path = "2025/02/file.txt"
    local_path = str(tmp_path)

    with patch(
        "viadot.sources.smb_client.download_file_from_smb",
        return_value=[str(tmp_path / "2025_02_file.txt")],
    ) as mock_download:
        result = smb_client_instance.download_to_local(
            path=remote_path,
            to_path=local_path,
            prefix_levels_to_add=2,
        )

        assert result is not None
        mock_download.assert_called_once()
        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs["prefix_levels_to_add"] == 2


def test_download_to_local_default_path(smb_client_instance, tmp_path):
    """Test download_to_local uses default path when to_path not specified."""
    remote_path = "data/file.txt"
    expected_local_file = str(
        tmp_path / "file.txt"
    )  # This would be in default /tmp/smb_source

    with patch(
        "viadot.sources.smb_client.download_file_from_smb",
        return_value=[expected_local_file],
    ) as mock_download:
        result = smb_client_instance.download_to_local(path=remote_path)

        assert result == [expected_local_file]
        mock_download.assert_called_once()
        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs["local_path"] == "/tmp/smb_source"  # noqa: S108


# ==================== to_bytes method tests ====================


def test_to_bytes_single_file(smb_client_instance):
    """Test to_bytes returns single file as dict."""
    remote_path = "data/file.txt"
    mock_content = b"file content"

    with patch("viadot.sources.smb_client.smbclient") as mock_smb:
        mock_file = mock_smb.open_file.return_value.__enter__.return_value
        mock_file.read.return_value = mock_content

        result = smb_client_instance.to_bytes(path=remote_path)

        assert result == {"file.txt": mock_content}
        mock_smb.open_file.assert_called_once()


def test_to_bytes_zip_file(smb_client_instance):
    """Test to_bytes handles ZIP files with filtering."""
    remote_path = "data/archive.zip"

    with (
        patch("viadot.sources.smb_client.smbclient") as _,
        patch("viadot.sources.smb_client.zipfile.ZipFile") as mock_zipfile_class,
    ):
        mock_zf = mock_zipfile_class.return_value.__enter__.return_value
        mock_zf.namelist.return_value = ["file1.csv", "file2.txt"]

        mock_member = mock_zf.open.return_value.__enter__.return_value
        mock_member.read.return_value = b"csv content"

        def mock_matches_any(text, patterns):
            return text.endswith(".csv") if patterns else True

        with patch(
            "viadot.sources.smb_client._matches_any_regex",
            side_effect=mock_matches_any,
        ):
            result = smb_client_instance.to_bytes(
                path=remote_path, zip_inner_file_regexes=["*.csv"]
            )

        expected = {"file1.csv": b"csv content"}
        assert result == expected


def test_to_bytes_path_required(smb_client_instance):
    """Test to_bytes raises TypeError when path not provided."""
    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'path'"
    ):
        smb_client_instance.to_bytes()


# ==================== Helper function tests ====================


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("normal_file.txt", False),
        ("file;with;semicolon.txt", True),
        ("file|with|pipe.txt", True),
        ("file`with`backtick.txt", True),
        ("file$with$dollar.txt", True),
        ("file.txt", False),
        ("file_with_underscore.txt", False),
    ],
)
def test_check_filename_for_problematic_chars(filename, expected):
    """Test checking for problematic characters in filenames."""
    result = _check_filename_for_problematic_chars(filename)
    assert result == expected


@pytest.mark.parametrize(
    ("file_path", "levels", "expected_prefix"),
    [
        ("/root/DATA/12345/file.txt", 0, ""),
        ("/root/DATA/12345/file.txt", 1, "12345"),
        ("/root/DATA/12345/subdir/file.txt", 2, "12345_subdir"),
        ("//root/DATA/file.txt", 2, "root_DATA"),
        ("/a/b/c/d/file.txt", 4, "a_b_c_d"),
        ("file.txt", 1, ""),
        ("\\root\\DATA\\1234\\file.txt", 2, "DATA_1234"),
        ("2025/02/file.xlsx", 2, "2025_02"),
        ("2025/02/file.xlsx", 1, "02"),
    ],
)
def test_build_prefix_from_path(file_path, levels, expected_prefix):
    """Test building prefix from file path."""
    result = _build_prefix_from_path(file_path, levels)
    assert result == expected_prefix


@pytest.mark.parametrize(
    ("filename", "prefix", "expected"),
    [
        ("file.txt", "", "file.txt"),
        ("file.txt", "12345", "12345_file.txt"),
        ("data.csv", "a_b", "a_b_data.csv"),
        ("report.xlsx", "2025_02", "2025_02_report.xlsx"),
    ],
)
def test_add_prefix_to_filename(filename, prefix, expected):
    """Test adding prefix to filename."""
    result = _add_prefix_to_filename(filename, prefix)
    assert result == expected


def test_get_unique_local_path_new_file(tmp_path):
    """Test getting unique local path for a new file."""
    filename = "test_file.txt"
    result = _get_unique_local_path(str(tmp_path), filename)
    expected = str(tmp_path / filename)
    assert result == expected
    assert not Path(result).exists()


def test_get_unique_local_path_existing_file(tmp_path):
    """Test getting unique local path when file already exists."""
    filename = "test_file.txt"
    existing_file = tmp_path / filename
    existing_file.write_text("existing content")

    result = _get_unique_local_path(str(tmp_path), filename)
    expected = str(tmp_path / "test_file_1.txt")
    assert result == expected
    assert result != str(existing_file)


def test_get_unique_local_path_multiple_existing_files(tmp_path):
    """Test getting unique local path when multiple files exist."""
    filename = "test_file.txt"
    # Create existing files
    (tmp_path / filename).write_text("content1")
    (tmp_path / "test_file_1.txt").write_text("content2")
    (tmp_path / "test_file_2.txt").write_text("content3")

    result = _get_unique_local_path(str(tmp_path), filename)
    expected = str(tmp_path / "test_file_3.txt")
    assert result == expected


# ==================== SMBItem tests ====================


def test_smb_item_initialization(mock_smb_item_file):
    """Test SMBItem initialization."""
    assert mock_smb_item_file.name == "test_file.txt"
    assert mock_smb_item_file.type == SMBItemType.FILE
    assert mock_smb_item_file.path == "test_file.txt"
    assert mock_smb_item_file.size == 1024
    assert isinstance(mock_smb_item_file.date_modified, datetime)


def test_smb_item_get_path(mock_smb_item_file):
    """Test SMBItem.get_path method."""
    assert mock_smb_item_file.get_path() == "test_file.txt"


def test_smb_item_to_dict(mock_smb_item_file):
    """Test SMBItem.to_dict method."""
    result = mock_smb_item_file.to_dict()
    assert isinstance(result, dict)
    assert result["name"] == "test_file.txt"
    assert result["type"] == "A"
    assert result["path"] == "test_file.txt"
    assert result["size"] == 1024
    assert "date_modified" in result
    # For files, children should be an empty list
    assert isinstance(result["children"], list)
    assert result["children"] == []


def test_smb_item_with_children(mock_smb_item_dir):
    """Test SMBItem with children."""
    child_file = SMBItem(
        name="child.txt",
        item_type=SMBItemType.FILE,
        path="test_folder/child.txt",
    )
    mock_smb_item_dir.children = [child_file]

    assert len(mock_smb_item_dir.children) == 1
    assert mock_smb_item_dir.children[0].name == "child.txt"


def test_smb_item_get_matching_file_paths_in_range_filename_regex(mock_smb_item_file):
    """Test SMBItem.get_matching_file_paths_in_range with filename_regex."""
    result = mock_smb_item_file.get_matching_file_paths_in_range(
        filename_regex=r"test.*\.txt"
    )
    assert len(result) == 1
    assert "test_file.txt" in result


def test_smb_item_get_matching_file_paths_in_range_extensions(mock_smb_item_file):
    """Test SMBItem.get_matching_file_paths_in_range with extensions filter."""
    result = mock_smb_item_file.get_matching_file_paths_in_range(extensions=[".txt"])
    assert len(result) == 1
    assert "test_file.txt" in result

    result_no_match = mock_smb_item_file.get_matching_file_paths_in_range(
        extensions=[".csv"]
    )
    assert len(result_no_match) == 0


def test_smb_item_get_matching_file_paths_in_range_date_filter(mock_smb_item_file):
    """Test SMBItem.get_matching_file_paths_in_range with date filter."""
    file_date = (
        mock_smb_item_file.date_modified.date()
        if mock_smb_item_file.date_modified
        else None
    )
    if file_date:
        result = mock_smb_item_file.get_matching_file_paths_in_range(
            date_filter=file_date.strftime("%Y-%m-%d")
        )
        assert len(result) == 1

        start_date = (file_date - pendulum.duration(days=1)).strftime("%Y-%m-%d")
        end_date = (file_date + pendulum.duration(days=1)).strftime("%Y-%m-%d")
        result_range = mock_smb_item_file.get_matching_file_paths_in_range(
            date_filter=(start_date, end_date)
        )
        assert len(result_range) == 1


def test_smb_item_get_matching_file_paths_in_range_exclude_directories(
    mock_smb_item_dir,
):
    """Test SMBItem.get_matching_file_paths_in_range with exclude_directories."""
    child_file = SMBItem(
        name="child.txt",
        item_type=SMBItemType.FILE,
        path="test_folder/child.txt",
    )
    mock_smb_item_dir.children = [child_file]

    result = mock_smb_item_dir.get_matching_file_paths_in_range(
        exclude_directories=True
    )
    # Should only return the file, not the directory
    assert len(result) == 1
    assert "test_folder/child.txt" in result
    assert "test_folder" not in result


def test_smb_item_get_matching_file_paths_in_range_full_path_prefix(
    mock_smb_item_file,
):
    """Test SMBItem.get_matching_file_paths_in_range with full_path_prefix."""
    result = mock_smb_item_file.get_matching_file_paths_in_range(
        full_path_prefix="//server/share/"
    )
    assert len(result) == 1
    assert result[0].startswith("//server/share/")


# ==================== SMBItem complex structure tests ====================


def test_smb_item_nested_structure():
    """Test SMBItem with nested directory structure."""
    root_dir = SMBItem(
        name="root",
        item_type=SMBItemType.DIRECTORY,
        path="root",
        children=[],
    )

    sub_dir = SMBItem(
        name="subdir",
        item_type=SMBItemType.DIRECTORY,
        path="root/subdir",
        children=[],
    )

    file1 = SMBItem(
        name="file1.txt",
        item_type=SMBItemType.FILE,
        path="root/subdir/file1.txt",
        size=100,
    )

    file2 = SMBItem(
        name="file2.txt",
        item_type=SMBItemType.FILE,
        path="root/file2.txt",
        size=200,
    )

    sub_dir.children = [file1]
    root_dir.children = [sub_dir, file2]

    assert len(root_dir.children) == 2
    assert len(sub_dir.children) == 1
    assert root_dir.children[0].name == "subdir"
    assert root_dir.children[1].name == "file2.txt"


def test_smb_item_get_matching_file_paths_in_range_recursive():
    """Test SMBItem.get_matching_file_paths_in_range with nested structure."""
    root_dir = SMBItem(
        name="root",
        item_type=SMBItemType.DIRECTORY,
        path="root",
        children=[],
    )

    file1 = SMBItem(
        name="report.txt",
        item_type=SMBItemType.FILE,
        path="root/report.txt",
        size=100,
    )

    file2 = SMBItem(
        name="data.csv",
        item_type=SMBItemType.FILE,
        path="root/data.csv",
        size=200,
    )

    root_dir.children = [file1, file2]

    result = root_dir.get_matching_file_paths_in_range(extensions=[".txt"])
    assert len(result) == 1
    assert "root/report.txt" in result
    assert "root/data.csv" not in result


def test_smb_item_get_matching_file_paths_in_range_filepath_regex():
    """Test SMBItem.get_matching_file_paths_in_range with filepath_regex."""
    file_item = SMBItem(
        name="file.txt",
        item_type=SMBItemType.FILE,
        path="data/2025/02/file.txt",
    )

    result = file_item.get_matching_file_paths_in_range(
        filename_regex="other",
        filepath_regex=r"2025/02",
    )
    assert len(result) == 1
    assert "data/2025/02/file.txt" in result


def test_smb_item_get_matching_file_paths_in_range_no_date():
    """Test SMBItem.get_matching_file_paths_in_range with item without date."""
    file_item = SMBItem(
        name="file.txt",
        item_type=SMBItemType.FILE,
        path="file.txt",
        date_modified=None,
    )

    # Should work without date
    result = file_item.get_matching_file_paths_in_range()
    assert len(result) == 1

    # Should not match date filter if no date
    result_with_date = file_item.get_matching_file_paths_in_range(
        date_filter="2025-01-15"
    )
    assert len(result_with_date) == 0


def test_smb_item_repr(mock_smb_item_file):
    """Test SMBItem.__repr__ method."""
    repr_str = repr(mock_smb_item_file)
    assert "test_file.txt" in repr_str
    assert "FILE" in repr_str or "A" in repr_str


# ==================== Edge cases and integration tests ====================


def test_build_prefix_from_path_edge_cases():
    """Test _build_prefix_from_path with edge cases."""
    # Test with more levels than available
    result = _build_prefix_from_path("a/b/file.txt", 10)
    assert result == "a_b"

    # Test with negative levels
    result = _build_prefix_from_path("a/b/file.txt", -1)
    assert result == ""

    # Test with empty path
    result = _build_prefix_from_path("", 1)
    assert result == ""

    # Test with root path
    result = _build_prefix_from_path("/file.txt", 1)
    assert result == ""


def test_add_prefix_to_filename_edge_cases():
    """Test _add_prefix_to_filename with edge cases."""
    # Test with empty prefix
    result = _add_prefix_to_filename("file.txt", "")
    assert result == "file.txt"

    # Test with None-like prefix (empty string)
    result = _add_prefix_to_filename("file.txt", "")
    assert result == "file.txt"


def test_smb_item_to_dict_without_optional_fields():
    """Test SMBItem.to_dict with minimal fields."""
    item = SMBItem(
        name="file.txt",
        item_type=SMBItemType.FILE,
        path="file.txt",
    )

    result = item.to_dict()
    assert result["name"] == "file.txt"
    assert "date_modified" not in result
    assert "size" not in result
    assert result["children"] == []


def test_smb_item_get_matching_file_paths_in_range_combined_filters():
    """Test SMBItem.get_matching_file_paths_in_range with multiple filters."""
    file_item = SMBItem(
        name="report_2025.pdf",
        item_type=SMBItemType.FILE,
        path="data/report_2025.pdf",
        date_modified=datetime(2025, 1, 15),
        size=1000,
    )

    # Test with all filters matching
    result = file_item.get_matching_file_paths_in_range(
        filename_regex=r"report.*",
        extensions=[".pdf"],
        date_filter="2025-01-15",
    )
    assert len(result) == 1

    # Test with one filter not matching
    result_no_match = file_item.get_matching_file_paths_in_range(
        filename_regex=r"report.*",
        extensions=[".csv"],  # Wrong extension
        date_filter="2025-01-15",
    )
    assert len(result_no_match) == 0
