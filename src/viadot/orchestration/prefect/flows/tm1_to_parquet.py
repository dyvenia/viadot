"""Flow for downloading data from TM1 to a Parquet file."""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet
from viadot.orchestration.prefect.tasks.tm1 import tm1_to_df


@flow(
    name="extract--tm1--parquet",
    description="Extract data from TM1 and load it into Parquet file",
    retries=1,
    retry_delay_seconds=60,
)
def tm1_to_parquet(  # noqa: PLR0913
    path: str | None = None,
    mdx_query: str | None = None,
    cube: str | None = None,
    view: str | None = None,
    limit: int | None = None,
    private: bool = False,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    if_empty: str = "skip",
    if_exists: Literal["append", "replace", "skip"] = "replace",
    verify: bool = True,
) -> None:
    """Download data from TM1 to a Parquet file.

    Args:
        mdx_query (str, optional): MDX select query needed to download the data.
            Defaults to None.
        cube (str, optional): Cube name from which data will be downloaded.
            Defaults to None.
        view (str, optional): View name from which data will be downloaded.
            Defaults to None.
        limit (str, optional): How many rows should be extracted.
            If None all the available rows will be downloaded. Defaults to None.
        private (bool, optional): Whether or not data download should be private.
            Defaults to False.
        credentials_secret (dict[str, Any], optional): The name of the secret that
            stores TM1 credentials.
            More info on: https://docs.prefect.io/concepts/blocks/. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to "TM1".
        if_empty (str, optional): What to do if output DataFrame is empty.
            Defaults to "skip".
        if_exists (Literal["append", "replace", "skip"], optional):
            What to do if the table exists. Defaults to "replace".
        verify (bool, optional): Whether or not verify SSL certificate.
            Defaults to False.
    """
    df = tm1_to_df(
        mdx_query=mdx_query,
        cube=cube,
        view=view,
        limit=limit,
        private=private,
        credentials_secret=credentials_secret,
        config_key=config_key,
        verify=verify,
        if_empty=if_empty,
    )
    return df_to_parquet(
        df=df,
        path=path,
        if_exists=if_exists,
    )
