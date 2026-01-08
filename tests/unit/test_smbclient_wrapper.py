"""Unit tests for smbclient_wrapper module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pendulum
from pydantic import SecretStr
import pytest

from viadot.exceptions import (
    CredentialError,
    SMBConnectionError,
    SMBFileOperationError,
)
from viadot.sources.smbclient_wrapper import (
    SMBClientWrapper,
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
def smb_wrapper_instance(valid_credentials):
    """Create an SMBClientWrapper instance for testing."""
    return SMBClientWrapper(
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


# ==================== SMBClientWrapper.__init__ tests ====================


def test_smb_wrapper_initialization_with_credentials(valid_credentials):
    """Test SMBClientWrapper initialization with credentials."""
    wrapper = SMBClientWrapper(
        server=SERVER,
        share=SHARE,
        smb_credentials=valid_credentials,
    )
    assert wrapper.server == SERVER
    assert wrapper.share == SHARE
    assert wrapper.credentials["username"] == USERNAME
    assert wrapper.credentials["password"].get_secret_value() == PASSWORD


def test_smb_wrapper_initialization_with_config_key():
    """Test SMBClientWrapper initialization with config_key."""
    mock_creds = {
        "username": USERNAME,
        "password": PASSWORD,
    }
    with patch(
        "viadot.sources.smbclient_wrapper.get_source_credentials",
        return_value=mock_creds,
    ):
        wrapper = SMBClientWrapper(
            server=SERVER,
            share=SHARE,
            config_key="test_config_key",
        )
        assert wrapper.server == SERVER
        assert wrapper.share == SHARE
        assert wrapper.credentials["username"] == USERNAME
        assert wrapper.credentials["password"].get_secret_value() == PASSWORD


def test_smb_wrapper_initialization_without_credentials():
    """Test SMBClientWrapper initialization fails without credentials."""
    with pytest.raises(
        CredentialError,
        match="`username`, and `password` credentials are required.",
    ):
        SMBClientWrapper(server=SERVER, share=SHARE)


# ==================== SMBClientWrapper.list_directory tests ====================


def test_list_directory_success(smb_wrapper_instance):
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
        "viadot.sources.smbclient_wrapper.get_hybrid_listing_with_fallback",
        return_value=mock_items,
    ) as mock_listing:
        result = smb_wrapper_instance.list_directory(directory="test_dir")

        assert len(result) == 2
        assert result[0].name == "file1.txt"
        assert result[1].name == "dir1"
        mock_listing.assert_called_once()
        call_kwargs = mock_listing.call_args.kwargs
        assert call_kwargs["server"] == SERVER
        assert call_kwargs["share"] == SHARE
        assert call_kwargs["start_directory"] == "test_dir"


def test_list_directory_with_skip_root_recursive(smb_wrapper_instance):
    """Test directory listing with skip_root_recursive option."""
    mock_items = [SMBItem(name="file.txt", item_type=SMBItemType.FILE, path="file.txt")]

    with patch(
        "viadot.sources.smbclient_wrapper.get_hybrid_listing_with_fallback",
        return_value=mock_items,
    ) as mock_listing:
        result = smb_wrapper_instance.list_directory(
            directory="", skip_root_recursive=True
        )

        assert len(result) == 1
        mock_listing.assert_called_once()
        call_kwargs = mock_listing.call_args.kwargs
        assert call_kwargs["skip_root_recursive"] is True


# ==================== SMBClientWrapper.download_file tests ====================


def test_download_file_success(smb_wrapper_instance, tmp_path):
    """Test successful file download."""
    remote_path = "data/file.txt"
    local_path = str(tmp_path)
    expected_local_file = str(tmp_path / "file.txt")

    with patch(
        "viadot.sources.smbclient_wrapper.download_file_from_smb_with_retry",
        return_value=expected_local_file,
    ) as mock_download:
        result = smb_wrapper_instance.download_file(
            remote_path=remote_path, local_path=local_path
        )

        assert result == expected_local_file
        mock_download.assert_called_once()
        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs["server"] == SERVER
        assert call_kwargs["share"] == SHARE
        assert call_kwargs["remote_path"] == remote_path
        assert call_kwargs["local_path"] == local_path


def test_download_file_with_prefix_levels(smb_wrapper_instance, tmp_path):
    """Test file download with prefix_levels_to_add."""
    remote_path = "2025/02/file.txt"
    local_path = str(tmp_path)

    with patch(
        "viadot.sources.smbclient_wrapper.download_file_from_smb_with_retry",
        return_value=str(tmp_path / "2025_02_file.txt"),
    ) as mock_download:
        result = smb_wrapper_instance.download_file(
            remote_path=remote_path,
            local_path=local_path,
            prefix_levels_to_add=2,
        )

        assert result is not None
        mock_download.assert_called_once()
        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs["prefix_levels_to_add"] == 2


def test_download_file_skipped_invalid_filename(smb_wrapper_instance, tmp_path):
    """Test file download returns None when filename has problematic chars."""
    remote_path = "data/file;test.txt"
    local_path = str(tmp_path)

    with patch(
        "viadot.sources.smbclient_wrapper._download_file_from_smb"
    ) as mock_download_inner:
        from viadot.exceptions import SMBInvalidFilenameError

        # _download_file_from_smb raises SMBInvalidFilenameError when invalid
        mock_download_inner.side_effect = SMBInvalidFilenameError(
            "Skipping file with problematic characters in name: file;test.txt"
        )

        # download_file_from_smb_with_retry should catch it and return None
        result = smb_wrapper_instance.download_file(
            remote_path=remote_path, local_path=local_path
        )

        assert result is None


def test_download_file_with_retry_params(smb_wrapper_instance, tmp_path):
    """Test file download with custom retry parameters."""
    remote_path = "data/file.txt"
    local_path = str(tmp_path)

    with patch(
        "viadot.sources.smbclient_wrapper.download_file_from_smb_with_retry",
        return_value=str(tmp_path / "file.txt"),
    ) as mock_download:
        smb_wrapper_instance.download_file(
            remote_path=remote_path,
            local_path=local_path,
            timeout=1800,
            max_retries=5,
            base_delay=5.0,
        )

        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs["timeout"] == 1800
        assert call_kwargs["max_retries"] == 5
        assert call_kwargs["base_delay"] == 5.0


# ==================== SMBClientWrapper.stream_files_to_s3 tests ====================


def test_stream_files_to_s3_success(smb_wrapper_instance):
    """Test successful streaming of files to S3."""
    smb_file_paths = ["file1.txt", "file2.txt"]
    s3_path = "s3://bucket/path/"
    aws_creds = {
        "aws_access_key_id": "test_key",
        "aws_secret_access_key": "test_secret",  # pragma: allowlist secret
    }
    expected_s3_paths = [
        "s3://bucket/path/file1.txt",
        "s3://bucket/path/file2.txt",
    ]

    with patch(
        "viadot.sources.smbclient_wrapper.stream_smb_files_to_s3",
        return_value=expected_s3_paths,
    ) as mock_stream:
        result = smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials=aws_creds,
        )

        assert len(result) == 2
        assert result == expected_s3_paths
        mock_stream.assert_called_once()
        call_kwargs = mock_stream.call_args.kwargs
        assert call_kwargs["smb_file_paths"] == smb_file_paths
        assert call_kwargs["s3_path"] == s3_path
        assert call_kwargs["aws_credentials"] == aws_creds


def test_stream_files_to_s3_without_aws_credentials(smb_wrapper_instance):
    """Test streaming to S3 without AWS credentials raises CredentialError."""
    smb_file_paths = ["file.txt"]
    s3_path = "s3://bucket/path/"

    with pytest.raises(CredentialError, match="must be a dictionary"):
        smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials=None,  # type: ignore
        )


def test_stream_files_to_s3_with_empty_aws_credentials(smb_wrapper_instance):
    """Test streaming to S3 with empty aws_credentials raises CredentialError."""
    smb_file_paths = ["file.txt"]
    s3_path = "s3://bucket/path/"

    with pytest.raises(
        CredentialError, match="aws_access_key_id.*aws_secret_access_key"
    ):
        smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials={},
        )


def test_stream_files_to_s3_with_incomplete_aws_credentials(smb_wrapper_instance):
    """Test streaming to S3 with incomplete aws_credentials raises CredentialError."""
    smb_file_paths = ["file.txt"]
    s3_path = "s3://bucket/path/"

    # Missing aws_secret_access_key
    with pytest.raises(
        CredentialError, match="aws_access_key_id.*aws_secret_access_key"
    ):
        smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials={"aws_access_key_id": "test_key"},
        )

    # Missing aws_access_key_id
    with pytest.raises(
        CredentialError, match="aws_access_key_id.*aws_secret_access_key"
    ):
        smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials={
                "aws_secret_access_key": "test_secret"  # pragma: allowlist secret
            },
        )


def test_stream_files_to_s3_with_invalid_aws_credentials_type(smb_wrapper_instance):
    """Test streaming to S3 with invalid aws_credentials type raises CredentialError."""
    smb_file_paths = ["file.txt"]
    s3_path = "s3://bucket/path/"

    # Invalid type (string instead of dict)
    with pytest.raises(CredentialError, match="must be a dictionary"):
        smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials="invalid",  # type: ignore
        )


def test_stream_files_to_s3_with_aws_credentials(smb_wrapper_instance):
    """Test streaming to S3 with AWS credentials."""
    smb_file_paths = ["file.txt"]
    s3_path = "s3://bucket/path/"
    aws_creds = {
        "aws_access_key_id": "test_key",
        "aws_secret_access_key": "test_secret",  # pragma: allowlist secret
    }

    with patch(
        "viadot.sources.smbclient_wrapper.stream_smb_files_to_s3",
        return_value=["s3://bucket/path/file.txt"],
    ) as mock_stream:
        smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials=aws_creds,
        )

        call_kwargs = mock_stream.call_args.kwargs
        assert call_kwargs["aws_credentials"] == aws_creds


def test_stream_files_to_s3_with_prefix_levels(smb_wrapper_instance):
    """Test streaming to S3 with prefix_levels_to_add."""
    smb_file_paths = ["2025/02/file.txt"]
    s3_path = "s3://bucket/path/"
    aws_creds = {
        "aws_access_key_id": "test_key",
        "aws_secret_access_key": "test_secret",  # pragma: allowlist secret
    }

    with patch(
        "viadot.sources.smbclient_wrapper.stream_smb_files_to_s3",
        return_value=["s3://bucket/path/2025_02_file.txt"],
    ) as mock_stream:
        smb_wrapper_instance.stream_files_to_s3(
            smb_file_paths=smb_file_paths,
            s3_path=s3_path,
            aws_credentials=aws_creds,
            prefix_levels_to_add=2,
        )

        call_kwargs = mock_stream.call_args.kwargs
        assert call_kwargs["prefix_levels_to_add"] == 2


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


# ==================== Retry logic tests ====================


def test_retry_with_exponential_backoff_success():
    """Test retry function succeeds on first attempt."""
    from viadot.sources.smbclient_wrapper import _retry_with_exponential_backoff

    mock_func = Mock(return_value="success")

    result = _retry_with_exponential_backoff(mock_func, max_retries=3)

    assert result == "success"
    assert mock_func.call_count == 1


def test_retry_with_exponential_backoff_retries_on_error():
    """Test retry function retries on SMBConnectionError."""
    from viadot.sources.smbclient_wrapper import _retry_with_exponential_backoff

    mock_func = Mock(side_effect=[SMBConnectionError("Error 1"), "success"])

    with patch("time.sleep"):  # Mock sleep to speed up test
        result = _retry_with_exponential_backoff(mock_func, max_retries=3)

        assert result == "success"
        assert mock_func.call_count == 2


def test_retry_with_exponential_backoff_raises_after_exhausted():
    """Test retry function raises exception after all retries exhausted."""
    from viadot.sources.smbclient_wrapper import _retry_with_exponential_backoff

    mock_func = Mock(side_effect=SMBFileOperationError("Persistent error"))

    with (
        patch("time.sleep"),  # Mock sleep to speed up test
        pytest.raises(SMBFileOperationError, match="Persistent error"),
    ):
        _retry_with_exponential_backoff(mock_func, max_retries=2)

    assert mock_func.call_count == 3  # Initial + 2 retries


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
