"""'test_sftp.py'."""

import json
from collections import defaultdict
from io import BytesIO, StringIO

import pandas as pd
import pytest

from viadot.exceptions import CredentialError
from viadot.sources import SftpConnector
from viadot.sources.sftp import SftpCredentials

variables = {
    "credentials": {
        "hostname": "",
        "username": "test_user",
        "password": "test_password",
        "port": 999,
        "rsa_key": "",
    },
}


class TestSftpCredentials:
    """Test SFTP Credentials Class."""

    @pytest.mark.basic()
    def test_sftp_credentials(self):
        """Test SFTP credentials."""
        SftpCredentials(
            hostname=variables["credentials"]["hostname"],
            username=variables["credentials"]["username"],
            password=variables["credentials"]["password"],
            port=variables["credentials"]["port"],
            rsa_key=variables["credentials"]["rsa_key"],
        )


@pytest.mark.basic()
def test_sftp_connector_initialization_without_credentials():
    """Test SFTP server without credentials."""
    with pytest.raises(CredentialError, match="Missing credentials."):
        SftpConnector(credentials=None)


@pytest.mark.connect()
def test_get_connection_without_rsa_key(mocker):
    """Test SFTP `get_connection` method without ras_key."""
    mock_transport = mocker.patch("viadot.sources.sftp.paramiko.Transport")
    mock_sftp_client = mocker.patch(
        "viadot.sources.sftp.paramiko.SFTPClient.from_transport"
    )

    connector = SftpConnector(credentials=variables["credentials"])
    connector.get_connection()

    mock_transport.assert_called_once_with((variables["credentials"]["hostname"], 999))
    mock_transport().connect.assert_called_once_with(
        None, variables["credentials"]["username"], variables["credentials"]["password"]
    )
    mock_sftp_client.assert_called_once()


@pytest.mark.connect()
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

    dummy_rsa_key = (
        "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAM3m...END RSA PRIVATE KEY-----"
    )
    keyfile = StringIO(dummy_rsa_key)
    credentials = variables["credentials"]
    credentials["rsa_key"] = keyfile.getvalue()

    connector = SftpConnector(credentials=variables["credentials"])
    connector.get_connection()

    mock_rsa_key.assert_called_once()
    mock_rsa_key.assert_called_once()
    mock_ssh_connect.assert_called_once_with(
        "", username="test_user", pkey=mock_rsa_key.return_value
    )
    assert connector.conn is not None


@pytest.mark.functions()
def test_to_df_with_csv(mocker):
    """Test SFTP `to_df` method with csv."""
    mock_getfo_file = mocker.patch.object(SftpConnector, "_getfo_file", autospec=True)
    mock_getfo_file.return_value = BytesIO(b"col1,col2\n1,2\n3,4\n")

    connector = SftpConnector(credentials=variables["credentials"])
    df = connector.to_df(file_name="test.csv", sep=",")

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 4)
    assert list(df.columns) == [
        "col1",
        "col2",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]


@pytest.mark.functions()
def test_to_df_with_tsv(mocker):
    """Test SFTP `to_df` method with tsv."""
    mock_getfo_file = mocker.patch.object(SftpConnector, "_getfo_file", autospec=True)
    mock_getfo_file.return_value = BytesIO(b"col1,col2\n1,2\n3,4\n")

    connector = SftpConnector(credentials=variables["credentials"])
    df = connector.to_df(file_name="test.tsv", sep=",")

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 4)
    assert list(df.columns) == [
        "col1",
        "col2",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]


@pytest.mark.functions()
def test_to_df_with_json(mocker):
    """Test SFTP `to_df` method with json."""
    mock_getfo_file = mocker.patch.object(SftpConnector, "_getfo_file", autospec=True)
    json_data = json.dumps({"col1": [1, 3], "col2": [2, 4]})
    mock_getfo_file.return_value = BytesIO(json_data.encode("utf-8"))

    connector = SftpConnector(credentials=variables["credentials"])
    df = connector.to_df(file_name="test.json")

    expected_df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
    expected_df["_viadot_source"] = "SftpConnector"
    expected_df["_viadot_downloaded_at_utc"] = pd.Timestamp.now()

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "col1",
        "col2",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]


@pytest.mark.functions()
def test_list_directory(mocker):
    """Test SFTP `_list_directory` method."""
    mock_sftp = mocker.MagicMock()
    mock_sftp.listdir.return_value = ["file1.txt", "file2.txt"]

    mock_get_connection = mocker.patch.object(
        SftpConnector, "get_connection", return_value=None
    )

    connector = SftpConnector(credentials=variables["credentials"])
    connector.conn = mock_sftp
    mock_close_conn = mocker.patch.object(SftpConnector, "_close_conn", autospec=True)
    files_list = connector._list_directory()

    assert files_list == ["file1.txt", "file2.txt"]
    mock_sftp.listdir.assert_called_once_with(".")


@pytest.mark.functions()
def test_recursive_listdir(mocker):
    """Test SFTP `_recursive_listdir` method."""

    mock_sftp = mocker.MagicMock()
    mock_attr = mocker.MagicMock()
    mock_attr.st_mode = 0
    mock_attr.filename = "subdir"
    mock_sftp.listdir_attr.return_value = [mock_attr]

    connector = SftpConnector(credentials=variables["credentials"])
    mocker.patch.object(connector, "conn", mock_sftp)
    result = connector._recursive_listdir("subdir")

    assert list(result) == ["subdir"]


@pytest.mark.functions()
def test_get_files_list(mocker):
    """Test SFTP `get_files_list` method."""
    mock_list_directory = mocker.patch.object(
        SftpConnector, "_list_directory", return_value=["file1.txt", "file2.txt"]
    )

    connector = SftpConnector(credentials=variables["credentials"])
    files = connector.get_files_list(path="test_path", recursive=False)

    assert files == ["file1.txt", "file2.txt"]
    mock_list_directory.assert_called_once_with(path="test_path")


@pytest.mark.functions()
def test_process_defaultdict_single_directory():
    """Test SFTP `_process_defaultdict` method."""
    connector = SftpConnector(credentials=variables["credentials"])
    data = defaultdict(list)
    data["dir1"].extend(["file1.txt", "file2.txt"])
    result = connector._process_defaultdict(data)
    expected = ["dir1/file1.txt", "dir1/file2.txt"]

    assert result == expected


@pytest.mark.functions()
def test_close_conn(mocker):
    """Test SFTP `_close_conn` method."""
    mock_conn = mocker.MagicMock()
    connector = SftpConnector(credentials=variables["credentials"])
    connector.conn = mock_conn
    connector._close_conn()

    mock_conn.close.assert_called_once()
    assert connector.conn is None
