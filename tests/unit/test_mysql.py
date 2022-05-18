from viadot.sources.mysql import MySQL
import pandas as pd
from unittest import mock


def test_create_mysql_instance():
    s = MySQL(credentials={"usr": 1, "pswd": 2})
    assert s


def test_connection_mysql():

    d = {"country": [1, 2], "sales": [3, 4]}
    df = pd.DataFrame(data=d)

    with mock.patch("viadot.sources.mysql.MySQL.to_df") as mock_method:
        mock_method.return_value = df

        assert type(df) == pd.DataFrame
