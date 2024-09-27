"""'test_sftp.py'."""

from io import BytesIO, StringIO
import json

import pandas as pd
import pytest

from viadot.exceptions import CredentialError
from viadot.sources import Sftp
from viadot.sources.sftp import SftpCredentials


variables = {
    "credentials": {
        "hostname": "",
        "username": "test_user",
        "password": "test_password",  # pragma: allowlist secret
        "port": 999,
        "rsa_key": "",
    },
}


class TestSftpCredentials:
    """Test SFTP Credentials Class."""

    @pytest.mark.basic
    def test_sftp_credentials(self):
        """Test SFTP credentials."""
        SftpCredentials(
            hostname=variables["credentials"]["hostname"],
            username=variables["credentials"]["username"],
            password=variables["credentials"]["password"],
            port=variables["credentials"]["port"],
            rsa_key=variables["credentials"]["rsa_key"],
        )


@pytest.mark.basic
def test_sftp_connector_initialization_without_credentials():
    """Test SFTP server without credentials."""
    with pytest.raises(CredentialError, match="Missing credentials."):
        Sftp(credentials=None)


@pytest.mark.connect
def test_get_connection_without_rsa_key(mocker):
    """Test `get_connection()` method without specifying the RSA key."""
    mock_transport = mocker.patch("viadot.sources.sftp.paramiko.Transport")
    mock_sftp_client = mocker.patch(
        "viadot.sources.sftp.paramiko.SFTPClient.from_transport"
    )

    connector = Sftp(credentials=variables["credentials"])
    connector.get_connection()

    mock_transport.assert_called_once_with((variables["credentials"]["hostname"], 999))
    mock_transport().connect.assert_called_once_with(
        None, variables["credentials"]["username"], variables["credentials"]["password"]
    )
    mock_sftp_client.assert_called_once()


@pytest.mark.connect
def test_get_connection_with_rsa_key(mocker):
    """Test SFTP `get_connection` method with ras_key."""
    mock_ssh_client = mocker.patch("viadot.sources.sftp.paramiko.SSHClient")
    mock_ssh_instance = mock_ssh_client.return_value
    mock_ssh_connect = mocker.patch.object(mock_ssh_instance, "connect")
    mock_rsa_key = mocker.patch(
        "viadot.sources.sftp.paramiko.RSAKey.from_private_key",
        return_value=mocker.Mock(),
    )
    mock_transport = mocker.Mock()

    # Ensure the SSHClient's transport attribute isn't None
    mock_ssh_instance._transport = mock_transport

    dummy_rsa_key = "test_rsa_key"
    keyfile = StringIO(dummy_rsa_key)
    credentials = variables["credentials"]
    credentials["rsa_key"] = keyfile.getvalue()

    connector = Sftp(credentials=variables["credentials"])
    connector.get_connection()

    mock_rsa_key.assert_called_once()
    mock_ssh_connect.assert_called_once_with(
        "", username="test_user", pkey=mock_rsa_key.return_value
    )
    assert connector.conn is not None


@pytest.mark.functions
def test_to_df_with_csv(mocker):
    """Test SFTP `to_df` method with csv."""
    mock_get_file_object = mocker.patch.object(Sftp, "_get_file_object", autospec=True)
    mock_get_file_object.return_value = BytesIO(b"col1,col2\n1,2\n3,4\n")

    connector = Sftp(credentials=variables["credentials"])
    df = connector.to_df(file_name="test.csv", sep=",")

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 4)
    assert list(df.columns) == [
        "col1",
        "col2",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]


@pytest.mark.functions
def test_to_df_with_json(mocker):
    """Test SFTP `to_df` method with json."""
    mock_get_file_object = mocker.patch.object(Sftp, "_get_file_object", autospec=True)
    json_data = json.dumps({"col1": [1, 3], "col2": [2, 4]})
    mock_get_file_object.return_value = BytesIO(json_data.encode("utf-8"))

    connector = Sftp(credentials=variables["credentials"])
    df = connector.to_df(file_name="test.json")

    expected_df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
    expected_df["_viadot_source"] = "Sftp"
    expected_df["_viadot_downloaded_at_utc"] = pd.Timestamp.now()

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "col1",
        "col2",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]


@pytest.mark.functions
def test_to_df_unsupported_file_type(mocker):
    """Test raising ValueError for unsupported file types."""
    mocker.patch.object(Sftp, "_get_file_object", return_value=BytesIO(b"dummy data"))
    sftp = Sftp(credentials=variables["credentials"])

    with pytest.raises(ValueError, match="Unable to read file"):
        sftp.to_df(file_name="test.txt")


@pytest.mark.functions
def test_to_df_empty_dataframe_warn(mocker, caplog):
    """Test handling of empty DataFrame with 'warn' option."""
    mocker.patch.object(Sftp, "_get_file_object", return_value=BytesIO(b"column"))
    sftp = Sftp(credentials=variables["credentials"])

    with caplog.at_level("INFO"):
        sftp.to_df(file_name="test.csv", if_empty="warn")
    assert "The response does not contain any" in caplog.text


@pytest.mark.functions
def test_ls(mocker):
    """Test SFTP `_ls` method."""
    mock_sftp = mocker.MagicMock()

    mock_sftp.listdir_attr.side_effect = [
        [
            mocker.MagicMock(st_mode=0o40755, filename="folder_a"),
            mocker.MagicMock(st_mode=0o100644, filename="file1.txt"),
            mocker.MagicMock(st_mode=0o100644, filename="file2.txt"),
        ],
        [
            mocker.MagicMock(st_mode=0o40755, filename="folder_b"),
            mocker.MagicMock(st_mode=0o100644, filename="file3.txt"),
        ],
    ]

    sftp = Sftp(credentials=variables["credentials"])
    sftp.conn = mock_sftp

    files_list = sftp._ls(path=".", recursive=False)

    assert len(files_list) == 2
    assert files_list == ["file1.txt", "file2.txt"]


@pytest.mark.functions
def test_recursive_ls(mocker):
    """Test SFTP recursive `_ls` method."""
    mock_sftp = mocker.MagicMock()
    mock_sftp.listdir_attr.side_effect = [
        [
            mocker.MagicMock(st_mode=0o40755, filename="folder_a"),
            mocker.MagicMock(st_mode=0o100644, filename="file1.txt"),
            mocker.MagicMock(st_mode=0o100644, filename="file2.txt"),
        ],
        [
            mocker.MagicMock(st_mode=0o40755, filename="folder_b"),
            mocker.MagicMock(st_mode=0o100644, filename="file3.txt"),
        ],
    ]

    sftp = Sftp(credentials=variables["credentials"])
    sftp.conn = mock_sftp

    files_list = sftp._ls(path=".", recursive=True)

    assert len(files_list) == 3
    assert files_list == ["folder_a/file3.txt", "file1.txt", "file2.txt"]


@pytest.mark.functions
def test_get_files_list(mocker):
    """Test SFTP `get_files_list` method."""
    mock_list_directory = mocker.patch.object(
        Sftp, "_ls", return_value=["file1.txt", "file2.txt"]
    )

    connector = Sftp(credentials=variables["credentials"])
    files = connector.get_files_list(path="test_path", recursive=False)

    assert files == ["file1.txt", "file2.txt"]
    mock_list_directory.assert_called_once_with(path="test_path", recursive=False)


@pytest.mark.functions
def test_close_conn(mocker):
    """Test SFTP `_close_conn` method."""
    mock_conn = mocker.MagicMock()
    connector = Sftp(credentials=variables["credentials"])
    connector.conn = mock_conn
    connector._close_conn()

    mock_conn.close.assert_called_once()
    assert connector.conn is None
