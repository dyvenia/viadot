"""Flow for downloading data from Business Core API to a Parquet file."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks.business_core import business_core_to_df
from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet


@flow(
    name="extract--businesscore--parquet",
    description="Extract data from Business Core API and load it into Parquet file",
    retries=1,
    retry_delay_seconds=60,
)
def business_core_to_parquet(
    path: str | None = None,
    url: str | None = None,
    filters_dict: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    verify: bool = True,
) -> None:
    """Download data from Business Core API to a Parquet file.

    Args:
        path (str, required): Path where to save the Parquet file. Defaults to None.
        url (str, required): Base url to the view in Business Core API.
            Defaults to None.
        filters_dict (Dict[str, Any], optional): Filters in form of dictionary.
            Available filters: 'BucketCount', 'BucketNo', 'FromDate', 'ToDate'.
            Defaults to None.
        credentials_secret (str, optional): The name of the secret that stores Business
            Core credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): Credential key to dictionary where details
            are stored. Defaults to None.
        if_empty (str, optional): What to do if output DataFrame is empty.
            Defaults to "skip".
        if_exists (Literal["append", "replace", "skip"], optional):
            What to do if the table exists. Defaults to "replace".
        verify (bool, optional): Whether or not verify certificates while
            connecting to an API. Defaults to True.
    """
    df = business_core_to_df(
        url=url,
        path=path,
        credentials_secret=credentials_secret,
        config_key=config_key,
        filters_dict=filters_dict,
        verify=verify,
    )
    return df_to_parquet(
        df=df,
        path=path,
        if_exists=if_exists,
    )
