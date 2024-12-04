from unittest.mock import patch

import pandas as pd
import pytest

from viadot.exceptions import ValidationError
from viadot.sources import TM1
from viadot.sources.tm1 import TM1Credentials


data = {"a": [420, 380, 390], "b": [50, 40, 45]}
test_cube = "test_cube"
test_view = "test_view"
test_dimension = "test_dim"
test_hierarchy = "test_hier"


@pytest.fixture
def tm1_credentials():
    return TM1Credentials(
        username="test_user",
        password="test_password",  # pragma: allowlist secret # noqa: S106
        address="localhost",
        port="12356",
    )


@pytest.fixture
def tm1(tm1_credentials: TM1Credentials):
    return TM1(
        credentials=tm1_credentials.dict(),
        verify=False,
    )


def test_tm1_initialization(tm1):
    """Test that the TM1 object is initialized with the correct credentials."""
    assert tm1.credentials.get("address") == "localhost"
    assert tm1.credentials.get("username") == "test_user"
    assert (
        tm1.credentials.get("password").get_secret_value()
        == "test_password"  # pragma: allowlist secret
    )


def test_get_cubes_names(tm1):
    with patch("viadot.sources.tm1.TM1Service") as mock:
        instance = mock.return_value
        instance.cubes.get_all_names.return_value = ["msg", "cuve"]
        result = tm1.get_cubes_names()
        assert result == ["msg", "cuve"]


def test_get_views_names(tm1):
    with patch("viadot.sources.tm1.TM1Service") as mock:
        instance = mock.return_value
        instance.views.get_all_names.return_value = ["view1", "view1"]
        result = tm1.get_views_names(cube=test_cube)
        assert result == ["view1", "view1"]


def test_get_dimensions_names(tm1):
    with patch("viadot.sources.tm1.TM1Service") as mock:
        instance = mock.return_value
        instance.dimensions.get_all_names.return_value = ["dim1", "dim2"]
        result = tm1.get_dimensions_names()
        assert result == ["dim1", "dim2"]


def test_get_available_elements(tm1):
    with patch("viadot.sources.tm1.TM1Service") as mock:
        instance = mock.return_value
        instance.elements.get_element_names.return_value = ["el1", "el2"]
        result = tm1.get_available_elements(
            dimension=test_dimension, hierarchy=test_hierarchy
        )
        assert result == ["el1", "el2"]


def test_to_df(tm1):
    with patch("viadot.sources.tm1.TM1Service") as mock:
        instance = mock.return_value
        instance.cubes.cells.execute_view_dataframe.return_value = pd.DataFrame(data)
        result = tm1.to_df(cube=test_cube, view=test_view)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) == 4


def test_to_df_fail(tm1, mocker):
    mocker.patch("viadot.sources.TM1.get_connection", return_value=True)
    with pytest.raises(ValidationError) as excinfo:
        tm1.to_df()
    assert str(excinfo.value) == "MDX query or cube and view are required."
