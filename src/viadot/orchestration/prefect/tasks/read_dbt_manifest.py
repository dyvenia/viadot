from typing import Any, Literal  # noqa: D100
from urllib.parse import urlparse

from prefect import get_run_logger, task

from viadot.sources import S3


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
    if store_type == "s3":
        return _read_manifest_from_s3(
            credentials=credentials,
            **store_kwargs,
        )
    msg = f"Manifest store type '{store_type}' is not supported."
    raise NotImplementedError(msg)


def _read_manifest_from_s3(
    credentials: dict[str, Any],
    path: str,
) -> dict:
    """Read the dbt manifest.json file from S3 and return it as a dict.

    Args:
        credentials: Store credentials.
        path: S3 path where manifest is stored (e.g. "my-bucket/manifest.json").
    """
    logger = get_run_logger()
    short_path = urlparse(path).path.lstrip("/")
    logger.info(f"Reading manifest from S3 path: {short_path}")
    s3 = S3(credentials=credentials)
    manifest = s3.to_dict(path=path)
    logger.info("Manifest succesfully loaded.")
    return manifest
