from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import ValidationError
from viadot.task_utils import credentials_loader
from viadot.sources import VidClub


CREDENTIALS = credentials_loader.run(credentials_secret="VIDCLUB")

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
def test_default_credential_param():
    vc = VidClub(credentials = CREDENTIALS)
    assert vc.credentials != None and type(vc.credentials) == dict

@pytest.mark.init
def test_create_club_class():
    vc = VidClub(credentials = CREDENTIALS)
    assert vc

@pytest.mark.proper
def test_build_query_wrong_source():
    with pytest.raises(
        ValidationError, match=r"Pick one these sources: jobs, product, company, survey"
    ):
        vc = VidClub(credentials = CREDENTIALS)
        query = vc.build_query(
            source="test",
            from_date="2023-03-24",
            to_date="2023-03-24",
            api_url="test",
            items_per_page=1,
        )


@pytest.mark.proper
def test_get_response_wrong_source():
    with pytest.raises(
        ValidationError, match=r"The source has to be: jobs, product, company or survey"
    ):
        vc = VidClub(credentials = CREDENTIALS)
        query = vc.get_response(source="test")


@mock.patch(
    "viadot.sources.vid_club.VidClub.get_response", return_value=MockClass.json()
)
@pytest.mark.parametrize("source", ["jobs", "company", "product", "survey"])
@pytest.mark.proper
def test_get_response_sources(mock_api_response, source):
    vc = VidClub(credentials = CREDENTIALS)
    query = vc.get_response(source=source, to_date="2023-03-24", from_date="2023-03-24")

    assert isinstance(query, pd.DataFrame)


@pytest.mark.proper
def test_get_response_wrong_date():
    with pytest.raises(
        ValidationError, match=r"from_date cannot be earlier than 2022-03-22"
    ):
        vc = VidClub(credentials = CREDENTIALS)
        query = vc.get_response(source="jobs", from_date="2021-05-09")


@pytest.mark.proper
def test_get_response_wrong_date_range():
    with pytest.raises(
        ValidationError, match=r"to_date cannot be earlier than from_date"
    ):
        vc = VidClub(credentials = CREDENTIALS)
        query = vc.get_response(
            source="jobs", to_date="2022-05-04", from_date="2022-05-05"
        )
