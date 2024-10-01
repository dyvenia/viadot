"""Test task `eurostat_to_df`."""

import pandas as pd
from prefect import flow

from viadot.orchestration.prefect.tasks import eurostat_to_df


def test_task_connection():
    """Function for testing eurstat connection."""

    @flow
    def test_eurostat_to_df():
        """Function for testing eurostat_to_df task."""
        df = eurostat_to_df(
            dataset_code="ILC_DI04",
            params={"hhtyp": "total", "indic_il": "med_e"},
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert list(df.columns) == [
            "geo",
            "time",
            "indicator",
            "label",
            "updated",
            "_viadot_source",
            "_viadot_downloaded_at_utc",
        ]

    test_eurostat_to_df()
