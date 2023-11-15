import pandas as pd
import pytest
from viadot.sources import TM1
from viadot.config import local_config
from viadot.exceptions import CredentialError, ValidationError

CUBE = local_config.get("TM1").get("test_cube")
VIEW = local_config.get("TM1").get("test_view")
DIMENSION = local_config.get("TM1").get("test_dim")
HIERARCHY = local_config.get("TM1").get("test_hierarchy")


def test_get_connection():
    tm1_source = TM1()
    connection = tm1_source.get_connection()

    assert connection is not None


def test_get_connection_fail():
    test_creds = {
        "address": "Addres",
        "port": 123,
        "username": "user",
    }
    with pytest.raises(CredentialError):
        tm1_source = TM1(credentials=test_creds)


def test_get_cubes_names():
    tm1_source = TM1()
    cubes = tm1_source.get_cubes_names()

    assert len(cubes) > 0


def test_get_dimensions_names():
    tm1_source = TM1()
    dim = tm1_source.get_dimensions_names()

    assert len(dim) > 0


def test_get_views_names():
    tm1_source = TM1(cube=CUBE)
    views = tm1_source.get_views_names()

    assert len(views) > 0


def test_get_hierarchies_names():
    tm1_source = TM1(dimension=DIMENSION)
    hierarchies = tm1_source.get_hierarchies_names()

    assert len(hierarchies) > 0


def test_get_available_elements():
    tm1_source = TM1(dimension=DIMENSION, hierarchy=HIERARCHY)
    elements = tm1_source.get_available_elements()

    assert len(elements) > 0


def test_to_df_view():
    tm1_source = TM1(cube=CUBE, view=VIEW)
    df = tm1_source.to_df()

    assert isinstance(df, pd.DataFrame)
    assert df.empty is False


def test_to_df_mdx():
    query = (
        """
        select
        {
        [version].[version].[Budget]
        }
        on columns,
        {
        [company].[company].MEMBERS
        }
        on rows

        FROM  """
        + f"{CUBE}"
    )

    tm1_source = TM1(mdx_query=query)
    df = tm1_source.to_df(if_empty="pass")

    assert isinstance(df, pd.DataFrame)


def test_to_df_fail_both():
    query = (
        """
        select
        {
        [version].[version].[Budget]
        }
        on columns,
        {
        [company].[company].MEMBERS
        }
        on rows

        FROM  """
        + f"{CUBE}"
    )

    tm1_source = TM1(mdx_query=query, cube=CUBE)
    with pytest.raises(
        ValidationError, match="Specify only one: MDX query or cube and view."
    ):
        tm1_source.to_df(if_empty="pass")


def test_to_df_fail_no():
    tm1_source = TM1()
    with pytest.raises(
        ValidationError, match="MDX query or cube and view are required."
    ):
        tm1_source.to_df(if_empty="pass")
