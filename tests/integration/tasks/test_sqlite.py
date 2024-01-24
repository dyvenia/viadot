import os
import sqlite3

import pandas as pd
import pytest

from viadot.tasks.sqlite import SQLiteInsert, SQLiteQuery, SQLiteSQLtoDF

TABLE = "test"


@pytest.fixture(scope="session")
def sqlite_insert():
    task = SQLiteInsert()
    yield task


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
    os.remove("testdb.sqlite")


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
    os.remove("testdb1.sqlite")


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
    os.remove("testdb2.sqlite")


def test_sqlite_sql_to_df(sqlite_insert):
    task = SQLiteSQLtoDF()
    with sqlite3.connect("testdb3.sqlite") as db:
        cursor = db.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS test ([AA] INT, [BB] INT) """)
        cursor.execute("""INSERT INTO test VALUES (11,22), (11,33)""")

    script = "SELECT * FROM test"
    with open("testscript.sql", "w") as file:
        file.write(script)
    df = task.run(db_path="testdb3.sqlite", sql_path="testscript.sql")
    assert isinstance(df, pd.DataFrame) == True
    expected = pd.DataFrame({"AA": [11, 11], "BB": [22, 33]})
    assert df.equals(expected) == True
    os.remove("testdb3.sqlite")
    os.remove("testscript.sql")


def test_sqlite_to_query(sqlite_insert):
    with sqlite3.connect("testdb4.sqlite") as db:
        cursor = db.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS test ([AA] INT, [BB] INT) """)
        query = "INSERT INTO test VALUES (11,22), (11,33)"
        task = SQLiteQuery()
        task.run(query=query, db_path="testdb4.sqlite")
        cursor.execute("""SELECT COUNT(*) from test """)
        result = cursor.fetchall()
        assert result[0][0] != 0
    os.remove("testdb4.sqlite")
