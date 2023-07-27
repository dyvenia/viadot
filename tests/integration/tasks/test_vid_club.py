from unittest import mock

import pandas as pd
import pytest

from viadot.task_utils import credentials_loader
from viadot.tasks import VidClubToDF

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
    """
    Checks if run method returns DataFrame.

    Args:
        var_dictionary: Dictionary with example arguments for run method.
    """
    vc_to_df = VidClubToDF(credentials=CREDENTIALS)

    df = vc_to_df.run(
        source=var_dictionary["source"],
        to_date=var_dictionary["to_date"],
        from_date=var_dictionary["from_date"],
        items_per_page=var_dictionary["items_per_page"],
        days_interval=var_dictionary["days_interval"],
    )

    assert isinstance(df, pd.DataFrame)


@pytest.mark.drop_cols
def test_drop_columns(var_dictionary):
    """
    Tests cols_to_drop argument in function.

    Args:
        var_dictionary: Dictionary with example arguments for run method.
    """
    cols_to_drop = ["regionID", "submissionDate"]
    vc_to_df = VidClubToDF(credentials=CREDENTIALS)

    output_with_dropped = vc_to_df.run(
        source=var_dictionary["source"],
        to_date=var_dictionary["to_date"],
        from_date=var_dictionary["from_date"],
        items_per_page=var_dictionary["items_per_page"],
        days_interval=var_dictionary["days_interval"],
        cols_to_drop=cols_to_drop,
    )

    assert all(col not in output_with_dropped.columns for col in cols_to_drop)


@pytest.mark.drop_cols
def test_drop_columns_KeyError(var_dictionary, caplog):
    """
    Tests if in case of KeyError (when passed columns in cols_to_drop are not included in DataFrame), there is returned error logger..

    Args:
        var_dictionary: Dictionary with example arguments for run method.
    """
    cols_to_drop = ["Test", "submissionDate"]
    vc_to_df = VidClubToDF(credentials=CREDENTIALS)

    vc_to_df.run(
        source=var_dictionary["source"],
        to_date=var_dictionary["to_date"],
        from_date=var_dictionary["from_date"],
        items_per_page=var_dictionary["items_per_page"],
        days_interval=var_dictionary["days_interval"],
        cols_to_drop=cols_to_drop,
    )
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "ERROR"
    assert (
        f"Column(s): {cols_to_drop} don't exist in the DataFrame"
        in caplog.records[0].message
    )


@pytest.mark.drop_cols
def test_drop_columns_TypeError(var_dictionary):
    """
    Tests raising TypeError if passed columns in cols_to_drop is not a List.

    Args:
        var_dictionary: Dictionary with example arguments for run method.
    """
    with pytest.raises(TypeError, match="Provide columns to drop in a List."):
        cols_to_drop = "Test"
        vc_to_df = VidClubToDF(credentials=CREDENTIALS)

        output_with_dropped = vc_to_df.run(
            source=var_dictionary["source"],
            to_date=var_dictionary["to_date"],
            from_date=var_dictionary["from_date"],
            items_per_page=var_dictionary["items_per_page"],
            days_interval=var_dictionary["days_interval"],
            cols_to_drop=cols_to_drop,
        )
