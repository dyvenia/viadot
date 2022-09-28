from viadot.sources.sftp import SftpConnector
from viadot.tasks.sftp import SftpToDF, SftpList
import pytest
import pandas as pd
from unittest import mock
from pytest import fixture, raises
import io


@pytest.fixture
def tmp_df():
    data = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=data)
    return df


@pytest.fixture
def list_of_paths():

    list_of_paths = [
        "Country__Context30##exported.tsv",
        "Country__Context31##exported.tsv",
        "Country__Context32##exported.tsv",
        "Country__Context33##exported.tsv",
        "Country__Context4##exported.tsv",
        "Country__Context6##exported.tsv",
        "Country__Context7##exported.tsv",
        "Country__Context8##exported.tsv",
        "Local_Products.csv",
        "Products Checkup.tsv",
        "Products.tsv",
        "RewardTest.csv",
    ]
    return list_of_paths


@pytest.fixture
def df_buf():
    s_buf = io.StringIO()

    data = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=data)
    df.to_csv(s_buf)
    return s_buf


def test_create_sftp_instance():
    s = SftpConnector(
        credentials_sftp={"HOSTNAME": 1, "USERNAME": 2, "PASSWORD": 3, "PORT": 4}
    )
    assert s


def test_connection_sftp(tmp_df):
    with mock.patch("viadot.sources.sftp.SftpConnector.to_df") as mock_method:
        mock_method.return_value = tmp_df
        s = SftpConnector(
            credentials_sftp={"HOSTNAME": 1, "USERNAME": 2, "PASSWORD": 3, "PORT": 4}
        )

        final_df = s.to_df()
        assert isinstance(final_df, pd.DataFrame)


def test_getfo_file(df_buf):
    with mock.patch("viadot.sources.sftp.SftpConnector.getfo_file") as mock_method:
        mock_method.return_value = df_buf
        s = SftpConnector(
            credentials_sftp={"HOSTNAME": 1, "USERNAME": 2, "PASSWORD": 3, "PORT": 4}
        )

        buffer_df = s.getfo_file()
        buffer_df.seek(0)
        df = pd.read_csv(buffer_df)
        assert isinstance(df, pd.DataFrame)


def test_ls_sftp(list_of_paths):
    with mock.patch("viadot.sources.sftp.SftpConnector.list_directory") as mock_method:
        mock_method.return_value = list_of_paths
        s = SftpConnector(
            credentials_sftp={"HOSTNAME": 1, "USERNAME": 2, "PASSWORD": 3, "PORT": 4}
        )

        paths = s.list_directory()

        assert isinstance(paths, list)


def test_get_exported_files(list_of_paths):
    with mock.patch.object(
        SftpConnector, "get_exported_files", return_value=list_of_paths
    ) as mock_method:

        s = SftpConnector(
            credentials_sftp={"HOSTNAME": 1, "USERNAME": 2, "PASSWORD": 3, "PORT": 4}
        )
        filtered_paths = s.get_exported_files()

        assert isinstance(filtered_paths, list)


def test_task_sftptodf(tmp_df):
    task = SftpToDF()
    with mock.patch.object(SftpToDF, "run", return_value=tmp_df) as mock_method:
        df = task.run()

        assert isinstance(df, pd.DataFrame)


def test_task_sftplist(list_of_paths):
    task = SftpList()
    with mock.patch.object(SftpList, "run", return_value=list_of_paths) as mock_method:
        list_directory = task.run()

        assert isinstance(list_directory, list)


def test_example():
    with mock.patch("viadot.sources.sftp.SftpConnector.to_df") as mock_method:
        mock_method.return_value = tmp_df
        t = SftpToDF()
        t.run()
