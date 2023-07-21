from unittest import mock

import pandas as pd
import pytest

from viadot.tasks import VidClubToDF
from viadot.task_utils import credentials_loader

CREDENTIALS = credentials_loader.run(credentials_secret="VIDCLUB")


class MockVidClubResponse:
    response_data = pd.DataFrame()


@pytest.fixture(scope="session")
def var_dictionary():
    variables = {
        "source": "jobs",
        "from_date": "2022-03-23",
        "to_date": "2022-03-24",
        "items_per_page": 1,
        "days_interval": 1,
    }
    yield variables


@mock.patch(
    "viadot.tasks.VidClubToDF.run", return_value=MockVidClubResponse.response_data
)
def test_vid_club_to_df(var_dictionary):
    source = var_dictionary["source"]
    from_date = var_dictionary["from_date"]
    to_date = var_dictionary["to_date"]
    items_per_page = var_dictionary["items_per_page"]
    days_interval = var_dictionary["days_interval"]

    vc_to_df = VidClubToDF(credentials=CREDENTIALS)
    df = vc_to_df.run(
        source=source,
        to_date=to_date,
        from_date=from_date,
        items_per_page=items_per_page,
        days_interval=days_interval,
    )

    assert isinstance(df, pd.DataFrame)
