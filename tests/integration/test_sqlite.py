import pytest
import pandas
from viadot.sources.sqlite import SQLite

TABLE = "test"


@pytest.fixture(scope="session")
def sqlite():

    sqlite = SQLite(driver="{SQLite}", server="localhost", db="testfile.sqlite")

    yield sqlite

    sqlite.run(f"DROP TABLE {TABLE}")


def test_conn_str(sqlite):
    assert (
        sqlite.conn_str == "DRIVER={SQLite};SERVER=localhost;DATABASE=testfile.sqlite;"
    )


def test_create_table(sqlite):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    result = sqlite.create_table(table=TABLE, dtypes=dtypes, if_exists="replace")
    assert result == True


def test_insert_into_sql(sqlite, DF):

    sql = sqlite.insert_into(TABLE, DF)

    assert "('italy', 100)" in sql
    assert sql[-1] == ";"


def test_insert_into_run(sqlite, DF):
    sql = sqlite.insert_into(TABLE, DF)
    sqlite.run(sql)
    results = sqlite.run("select * from test")
    df = pandas.DataFrame.from_records(results, columns=["country", "sales"])
    assert df["sales"].sum() == 230
