import types

import pytest

from viadot.sources.postgres import PostgreSQL


@pytest.fixture
def postgres_credentials():
    return {
        "user": "test_user",
        "password": "test_password",  # pragma: allowlist secret
        "server": "localhost",
        "db_name": "test_db",
        "driver": "PostgreSQL Unicode",
        "port": 5432,
        "sslmode": "require",
    }


def test_postgres_initialization(postgres_credentials):
    pg = PostgreSQL(credentials=postgres_credentials)
    assert pg.credentials["server"] == "localhost"
    assert pg.credentials["user"] == "test_user"
    assert (
        pg.credentials["password"]
        == "test_password"  # pragma: allowlist secret  # noqa: S105
    )
    assert pg.credentials["db_name"] == "test_db"
    assert pg.credentials["driver"] == "PostgreSQL Unicode"
    assert pg.credentials["port"] == 5432


def test_conn_str_includes_sslmode(postgres_credentials):
    pg = PostgreSQL(credentials=postgres_credentials)
    conn_str = pg.conn_str
    assert "DRIVER={PostgreSQL Unicode};" in conn_str
    assert "SERVER=localhost;" in conn_str
    assert "PORT=5432;" in conn_str
    assert "DATABASE=test_db;" in conn_str
    assert "UID=test_user;" in conn_str
    assert "PWD=test_password;" in conn_str  # pragma: allowlist secret
    assert "SSLmode=require;" in conn_str


def test_check_if_table_exists_calls_run(mocker, postgres_credentials):
    pg = PostgreSQL(credentials=postgres_credentials)
    mock_run = mocker.patch("viadot.sources.base.SQL.run", return_value=[(1,)])
    assert pg._check_if_table_exists(table="my_table", schema="public")
    assert mock_run.called


def test_con_ignores_unsupported_timeout(monkeypatch, postgres_credentials):
    # Create a dummy pyodbc-like module
    class UnsupportedAttributeError(Exception):
        pass

    class DummyConn:
        def __init__(self):
            self._timeout_set = False

        @property
        def timeout(self):
            return 0

        @timeout.setter
        def timeout(self, _value):
            # Simulate driver not supporting this attribute
            msg = "unsupported attribute"
            raise UnsupportedAttributeError(msg)

    def dummy_connect(_conn_str, _timeout=0):
        return DummyConn()

    dummy_pyodbc = types.SimpleNamespace(connect=dummy_connect)

    def fake_import(name, *args, **kwargs):
        if name == "pyodbc":
            return dummy_pyodbc
        return __import__(name, *args, **kwargs)

    import builtins

    monkeypatch.setattr(builtins, "__import__", fake_import)

    pg = PostgreSQL(credentials=postgres_credentials)
    con = pg.con
    assert isinstance(con, DummyConn)
