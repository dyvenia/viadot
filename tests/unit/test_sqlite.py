import pandas
import pytest

from viadot.sources.sqlite import SQLite

TABLE = "test"


@pytest.fixture(scope="session")
def sqlite():

    sqlite = SQLite(credentials=dict(db_name="testfile.sqlite"), query_timeout=5)

    yield sqlite

    sqlite.run(f"DROP TABLE {TABLE}")


def test_conn_str(sqlite):
    DRIVER = "/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so"
    assert (
        sqlite.conn_str
        == f"DRIVER={{{DRIVER}}};SERVER=localhost;DATABASE=testfile.sqlite;"
    )


def test_create_table(sqlite):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    result = sqlite.create_table(table=TABLE, dtypes=dtypes, if_exists="replace")
    assert result == True


def test_insert_into_sql(sqlite, DF):

    sql = sqlite.insert_into(TABLE, DF)

    assert "('italy', 100)" in sql
    assert sql[-1] == ";"

    results = sqlite.run(f"SELECT * FROM {TABLE}")
    df = pandas.DataFrame.from_records(results, columns=["country", "sales"])
    assert df["sales"].sum() == 230
