"""Power bi report scanner to Pandas dataframes tasks."""

import pandas as pd
from prefect import task

from viadot.sources.power_bi import PowerBiWorkspaceInfo


@task
def power_bi_workspace_info_to_dict(
    target_date: str | None = None,
    power_bi_credential_secret: str | None = None,
) -> dict[str, pd.DataFrame]:
    """Parse the scan results into a dictionary of DataFrames."""
    pbi = PowerBiWorkspaceInfo(
        target_date=target_date, credential_secret=power_bi_credential_secret
    )
    return pbi.to_dict()
