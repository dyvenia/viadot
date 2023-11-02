import pandas as pd

from viadot.sources import TM1
from viadot.config import local_config

CUBE = local_config.get("test_cube")
VIEW = local_config.get("test_view")


def test_get_connection():
    tm1_source = TM1()
    connection = tm1_source.get_connection()

    assert connection is not None


def test_get_cubes_names():
    tm1_source = TM1()
    cubes = tm1_source.get_cubes_names()

    assert len(cubes) > 0


def test_get_cubes_names():
    tm1_source = TM1(cube=CUBE)
    views = tm1_source.get_views_names()

    assert len(views) > 0


def test_to_df():
    tm1_source = TM1(cube=CUBE, view=VIEW)
    df = tm1_source.to_df()

    assert isinstance(df, pd.DataFrame)
    assert df.empty is False
