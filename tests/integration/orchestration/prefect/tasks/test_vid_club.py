import pandas as pd
import pytest

from src.viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from src.viadot.orchestration.prefect.tasks import vid_club_to_df


EXPECTED_DF = pd.DataFrame(
    {"id": [1, 2], "name": ["Company A", "Company B"], "region": ["pl", "ro"]}
)


class MockVidClub:
    def __init__(self, *args, **kwargs):
        """Init method."""
        pass

    def to_df(self):
        return EXPECTED_DF


def test_vid_club_to_df(mocker):
    mocker.patch("viadot.orchestration.prefect.tasks.VidClub", new=MockVidClub)

    df = vid_club_to_df(
        endpoint="company",
        from_date="2023-01-01",
        to_date="2023-12-31",
        items_per_page=100,
        region="pl",
        vidclub_credentials_secret="VIDCLUB",  # pragma: allowlist secret # noqa: S106
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.equals(EXPECTED_DF)


def test_vid_club_to_df_missing_credentials(mocker):
    mocker.patch(
        "viadot.orchestration.prefect.tasks.get_credentials", return_value=None
    )

    with pytest.raises(MissingSourceCredentialsError):
        vid_club_to_df(
            endpoint="company",
            from_date="2023-01-01",
            to_date="2023-12-31",
            items_per_page=100,
            region="pl",
            vidclub_credentials_secret="VIDCLUB",  # pragma: allowlist secret # noqa: S106
        )
