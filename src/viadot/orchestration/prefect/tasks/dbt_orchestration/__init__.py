from typing import Any, Literal

from prefect import get_run_logger, task

from viadot.orchestration.dbt_dynamic.manifest_store import (
    ManifestStore,
)


@task(retries=1, retry_delay_seconds=10)
def read_dbt_manifest(
    credentials: dict[str, Any],
    store_type: Literal["s3"] = "s3",
    **store_kwargs,
) -> dict:
    """Read the dbt manifest from the specified store and return it as a dict.

    Args:
        credentials: Store credentials.
        store_type: Type of the storage (e.g. "s3").
    """
    logger = get_run_logger()
    logger.info("Reading dbt manifest from configured store.")
    store = ManifestStore(store_type=store_type)
    return store.read(
        credentials=credentials,
        **store_kwargs,
    )
