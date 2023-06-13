from unittest import mock

import pandas as pd
import pytest

from viadot.tasks.vid_club import VidClubToDF


class MockVidClubResponse:
    response_data = pd.DataFrame()


@pytest.fixture(scope="session")
def var_dictionary():
    variables = {"source": "jobs", "from_date": "2022-03-23", "to_date": "2022-03-24"}
    yield variables


@mock.patch.object("VidClubToDF.run", mock_run=MockVidClubResponse.response_data)
def test_vid_club_to_df(mock_run, var_dictionary):
    source = var_dictionary["source"]
    from_date = var_dictionary["from_date"]
    to_date = var_dictionary["to_date"]

    vc_to_df = VidClubToDF(
        source=source,
        to_date=to_date,
        from_date=from_date,
    )
    df = vc_to_df.run()

    assert isinstance(df, pd.DataFrame)
