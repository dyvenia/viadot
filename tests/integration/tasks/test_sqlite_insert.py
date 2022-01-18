import sqlite3
import os
import pytest
import pandas as pd

from viadot.tasks.sqlite import SQLiteInsert

TABLE = "test"


@pytest.fixture(scope="session")
def sqlite_insert():
    task = SQLiteInsert()
    yield task
    os.remove("testdb.sqlite")
    os.remove("testdb1.sqlite")
    os.remove("testdb2.sqlite")


def test_sqlite_insert_proper(sqlite_insert):
    dtypes = {"AA": "INT", "BB": "INT"}
    df2 = pd.DataFrame({"AA": [1, 2, 3], "BB": [11, 22, 33]})
    sqlite_insert.run(
        table_name=TABLE,
        df=df2,
        dtypes=dtypes,
        if_exists="skip",
        db_path="testdb.sqlite",
    )

    with sqlite3.connect("testdb.sqlite") as db:
        cursor = db.cursor()
        cursor.execute("""SELECT COUNT(*) from test """)
        result = cursor.fetchall()
        assert result[0][0] != 0


def test_sqlite_insert_empty(caplog, sqlite_insert):
    df = pd.DataFrame()
    dtypes = {"AA": "INT", "BB": "INT"}
    sqlite_insert.run(
        table_name=TABLE,
        df=df,
        dtypes=dtypes,
        if_exists="skip",
        db_path="testdb1.sqlite",
    )

    with sqlite3.connect("testdb1.sqlite") as db:
        cursor2 = db.cursor()
        cursor2.execute("""SELECT COUNT(*) from test """)
        result2 = cursor2.fetchall()
        assert result2[0][0] == 0

    assert "DataFrame is empty" in caplog.text


def test_sqlite_insert_not(caplog, sqlite_insert):
    dtypes = {"AA": "INT", "BB": "INT"}
    not_df = []
    sqlite_insert.run(
        table_name=TABLE,
        df=not_df,
        dtypes=dtypes,
        if_exists="skip",
        db_path="testdb2.sqlite",
    )
    with sqlite3.connect("testdb2.sqlite") as db:
        cursor3 = db.cursor()
        cursor3.execute("""SELECT COUNT(*) from test """)
        result3 = cursor3.fetchall()
        assert result3[0][0] == 0

    assert "Object is not a pandas DataFrame" in caplog.text
