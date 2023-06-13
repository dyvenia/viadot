import pytest
import pandas as pd

from unittest import mock

from viadot.sources import VidClub


@pytest.fixture
def var_dictionary():
    variables = {}

    return variables


class MockClass:
    status_code = 200

    def json():
        df = pd.DataFrame()
        return df


@pytest.mark.init
def test_create_club_class():
    vc = VidClub()
    assert vc


@pytest.mark.init
def test_default_credential_param():
    vc = VidClub()
    assert vc.credentials != None and type(vc.credentials) == dict


@pytest.mark.proper
def test_build_query_wrong_source():
    with pytest.raises(Exception):
        vc = VidClub()
        query = vc.build_query(source="test")


@pytest.mark.proper
def test_get_response_wrong_source():
    with pytest.raises(Exception):
        vc = VidClub()
        query = vc.get_response(source="test")


@mock.patch(
    "viadot.sources.vid_club.VidClub.get_response", return_value=MockClass.json()
)
@pytest.mark.parametrize("source", ["jobs", "company", "product", "survey"])
@pytest.mark.proper
def test_get_response_sources(mock_api_response, source):
    vc = VidClub()
    query = vc.get_response(source=source, to_date="2023-03-24", from_date="2023-03-24")

    assert isinstance(query, pd.DataFrame)


@pytest.mark.proper
def test_get_response_wrong_date():
    with pytest.raises(Exception):
        vc = VidClub()
        query = vc.get_response(source="jobs", to_date="2021-05-09")


@pytest.mark.proper
def test_get_response_wrong_date_range():
    with pytest.raises(Exception):
        vc = VidClub()
        query = vc.get_response(
            source="jobs", to_date="2022-05-04", from_date="2022-05-05"
        )
