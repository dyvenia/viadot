from unittest import mock

import pandas as pd
import pytest
from datetime import datetime, timedelta

from viadot.exceptions import ValidationError
from viadot.task_utils import credentials_loader
from viadot.sources import VidClub


CREDENTIALS = credentials_loader.run(credentials_secret="VIDCLUB")
vc = VidClub(credentials=CREDENTIALS)


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
    assert vc.credentials != None and type(vc.credentials) == dict


@pytest.mark.init
def test_create_club_class():
    assert vc


@pytest.mark.proper
def test_build_query_wrong_source():
    with pytest.raises(
        ValidationError, match=r"Pick one these sources: jobs, product, company, survey"
    ):
        query = vc.build_query(
            source="test",
            from_date="2023-03-24",
            to_date="2023-03-24",
            api_url="test",
            items_per_page=1,
        )


@pytest.mark.intervals
def test_intervals_split():
    from_date = "2022-01-01"
    to_date = "2022-01-19"
    days_interval = 5
    expected_starts = ["2022-01-01", "2022-01-06", "2022-01-11", "2022-01-16"]
    expected_ends = ["2022-01-06", "2022-01-11", "2022-01-16", "2022-01-19"]
    starts, ends = vc.intervals(
        from_date=from_date, to_date=to_date, days_interval=days_interval
    )

    assert starts == expected_starts
    assert ends == expected_ends


@mock.patch("viadot.sources.vid_club.VidClub.check_connection")
@pytest.mark.connection_check
def test_check_connection(mock_function):
    mock_first_output = "Mocked Output"
    mock_second_output = "Mocked Output"
    mock_function.return_value = mock_first_output, mock_function.return_value[1]

    response, first_url = vc.check_connection(
        source="jobs",
        from_date="2023-03-24",
        to_date="2023-03-24",
        url="test",
        items_per_page=1,
    )
    print(response)
    print(first_url)
    assert isinstance(first_url, str)
    assert len(first_url) > len(mock_second_output)


@pytest.mark.proper
def test_get_response_wrong_source():
    with pytest.raises(
        ValidationError, match=r"The source has to be: jobs, product, company or survey"
    ):
        query = vc.get_response(source="test")


@mock.patch(
    "viadot.sources.vid_club.VidClub.get_response", return_value=MockClass.json()
)
@pytest.mark.parametrize("source", ["jobs", "company", "product", "survey"])
@pytest.mark.proper
def test_get_response_sources(mock_api_response, source):
    query = vc.get_response(source=source, to_date="2023-03-24", from_date="2023-03-24")

    assert isinstance(query, pd.DataFrame)
    mock_api_response.assert_called_once()


@pytest.mark.proper
def test_get_response_wrong_date():
    with pytest.raises(
        ValidationError, match=r"from_date cannot be earlier than 2022-03-22"
    ):
        query = vc.get_response(source="jobs", from_date="2021-05-09")


@pytest.mark.proper
def test_get_response_wrong_date_range():
    with pytest.raises(
        ValidationError, match=r"to_date cannot be earlier than from_date"
    ):
        query = vc.get_response(
            source="jobs", to_date="2022-05-04", from_date="2022-05-05"
        )


@pytest.mark.total_load
def test_total_load_for_the_same_dates():
    from_date = "2022-04-01"
    to_date = "2022-04-01"
    days_interval = 10
    source = "jobs"
    df = vc.total_load(
        from_date=from_date, to_date=to_date, days_interval=days_interval, source=source
    )

    assert isinstance(df, pd.DataFrame)


@pytest.mark.total_load
def test_total_load_for_intervals():
    from_date = "2022-04-01"
    to_date = "2022-04-12"
    days_interval = 2
    source = "jobs"

    date_object = datetime.strptime(from_date, "%Y-%m-%d") + timedelta(
        days=days_interval
    )
    one_interval = date_object.strftime("%Y-%m-%d")

    df = vc.total_load(
        from_date=from_date, to_date=to_date, days_interval=days_interval, source=source
    )
    df_one_interval = vc.total_load(
        from_date=from_date,
        to_date=one_interval,
        days_interval=days_interval,
        source=source,
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) > len(df_one_interval)


@pytest.mark.total_load
def test_drop_duplicates():
    from_date = "2022-04-01"
    to_date = "2022-04-12"
    days_interval = 2
    source = "jobs"

    df = vc.total_load(
        from_date=from_date, to_date=to_date, days_interval=days_interval, source=source
    )
    dups_mask = df.duplicated()
    df_check = df[dups_mask]

    assert len(df_check) == 0
