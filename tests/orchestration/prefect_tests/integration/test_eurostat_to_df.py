import pandas as pd
from prefect import flow
from viadot.orchestration.prefect.tasks import eurostat_to_df


def test_task_connexion():
    @flow
    def test_eurostat_to_df():
        df = eurostat_to_df(
            dataset_code="ILC_DI04",
            params={"hhtyp": "total", "indic_il": "med_e"},
            requested_columns=["updated", "geo", "indicator"],
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert list(df.columns) == [
            "updated",
            "geo",
            "indicator",
            "_viadot_source",
            "_viadot_downloaded_at_utc",
        ]

    test_eurostat_to_df()
