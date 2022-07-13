from unittest import mock

import pandas as pd

from viadot.sources.mysql import MySQL

d = {"country": [1, 2], "sales": [3, 4]}
df = pd.DataFrame(data=d)

query = """SELECT * FROM `example-views`.`sales`"""


def test_create_mysql_instance():
    s = MySQL(credentials={"usr": 1, "pswd": 2})
    assert s


def test_connection_mysql():
    with mock.patch("viadot.sources.mysql.MySQL.to_df") as mock_method:
        mock_method.return_value = df
        s = MySQL(credentials={"usr": 1, "pswd": 2})

        final_df = s.to_df(query=query)
        assert type(final_df) == pd.DataFrame


def test_connect_mysql_ssh():
    with mock.patch("viadot.sources.mysql.MySQL.connect_sql_ssh") as mock_method:
        mock_method.return_value = df
        s = MySQL(credentials={"usr": 1, "pswd": 2})

        final_df = s.connect_sql_ssh(query=query)
        assert type(final_df) == pd.DataFrame
