from datetime import datetime, timedelta
from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import ValidationError
from viadot.sources import VidClub
from viadot.task_utils import credentials_loader

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
    """
    Checks if credentials are loaded from Azure Key Vault or PrefectSecret or from local config ursing credentials_loader and if it's dictionary type.
    """
    assert vc.credentials is not None and isinstance(vc.credentials, dict)


@pytest.mark.build_query
def test_build_query_wrong_source():
    """
    Checks if passing different source than Literal["jobs", "product", "company", "survey"] is catched and returns error.
    """
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


@pytest.mark.build_query
def test_url_string():
    """
    Checks if fucntion generates URL with needed parameters.
    """
    source = "jobs"
    from_date = "2023-03-24"
    to_date = "2023-03-24"
    api_url = "https://api/test/"
    items_per_page = 1

    expected_elements = [
        f"from={from_date}",
        f"to={to_date}",
        "region=all",
        f"limit={items_per_page}",
        api_url,
    ]

    query = vc.build_query(
        source=source,
        from_date=from_date,
        to_date=to_date,
        api_url=api_url,
        items_per_page=items_per_page,
    )

    assert all(ex in query for ex in expected_elements)


@pytest.mark.intervals
def test_intervals_split():
    """
    Checks if prrovided date range with days_interval creates list with expected split.
    """
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


@pytest.mark.connection_check
def test_check_connection():
    """
    Checks if check_connection method returns tuple with dictionary and string.
    """
    output = vc.check_connection(
        source="jobs",
        from_date="2023-03-24",
        to_date="2023-03-24",
        items_per_page=1,
    )

    response, first_url = vc.check_connection(
        source="jobs",
        from_date="2023-03-24",
        to_date="2023-03-24",
        items_per_page=1,
    )

    assert isinstance(output, tuple)
    assert isinstance(response, dict)
    assert isinstance(first_url, str)


@pytest.mark.proper
def test_get_response_wrong_source():
    """
    Checks if ValidationError is returned when passing wrong source name.
    """
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
    """
    Checks if get_response method returnes DataFrame for each of the 4 possible sources.
    Test assert that the mock was called exactly once.

    Args:
        mock_api_response: Mocked return_value for get_response method.
        source: The endpoint source to be accessed.
    """
    query = vc.get_response(source=source, to_date="2023-03-24", from_date="2023-03-24")

    assert isinstance(query, pd.DataFrame)
    mock_api_response.assert_called_once()


@pytest.mark.proper
def test_get_response_wrong_date():
    """
    Checks if ValidationError is returned when passing from_date earlier than 2022-03-22.
    """
    with pytest.raises(
        ValidationError, match=r"from_date cannot be earlier than 2022-03-22"
    ):
        vc.get_response(source="jobs", from_date="2021-05-09")


@pytest.mark.proper
def test_get_response_wrong_date_range():
    """
    Checks if ValidationError is returned when passing to_date earlier than from_date.
    """
    with pytest.raises(
        ValidationError, match=r"to_date cannot be earlier than from_date"
    ):
        vc.get_response(source="jobs", to_date="2022-05-04", from_date="2022-05-05")


@pytest.mark.total_load
def test_total_load_for_the_same_dates():
    """
    total_load method includes logic for situation when from_date == to_date. In this scenario interval split is skipped and used just get_response method.
    This test checks if this logic is executed without error and returned object if DataFrame.
    """
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
    """
    Checks if interval function is properly looped in the total_load method. At first we check if returned object is DataFrame,
    then we check if returned DataFrame for smaller date range contains less rows than DataFrame returned for bigger date range.
    """
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
    """
    Checks logic for dropping duplicated rows, that is included in total_load method.
    Test checks if returned DataFrame has duplicates.
    """
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
