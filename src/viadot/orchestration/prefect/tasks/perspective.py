"""Tasks for interacting with [Perspective](https://github.com/dyvenia/perspective)."""

import os
import subprocess

from prefect import task
from prefect.logging import get_run_logger


@task
def perspective_ingest_task(
    perspective_api_url: str | None = None,
    perspective_api_token: str | None = None,
    target_path: str | None = None,
    follow: bool = False,
    dry_run: bool = False,
) -> subprocess.CompletedProcess:
    """Upload test results to Perspective.

    Args:
        perspective_api_url (str | None): URL of the Perspective catalog to which test
            results will be sent. Defaults to None.
        perspective_api_token (str | None): Token used for communication with
            Perspective. Defaults to None.
        target_path (str | None): Path to dbt target directory in which run_results.json
            file is present. Defaults to None.
        follow (bool): Whether --follow flag should be added to `perspective ingest`
            command. Defaults to False.
        dry_run (bool): Whether --dry-run flag should be added to `perspective ingest`
            command. Defaults to False.

    Returns:
        subprocess.CompletedProcess: Information about finished process.

    Raises:
        Exception: In case something goes wrong with `perspective ingest`.

    """
    cmd = [
        "perspective",
        "ingest",
        "dbt",
        "--test-results",
    ]
    if perspective_api_url:
        cmd += ["--url", perspective_api_url]
    if target_path:
        cmd += ["--metadata-dir", target_path]
    if dry_run:
        cmd += ["--dry-run"]
    elif follow:
        cmd += ["--follow"]

    logger = get_run_logger()
    logger.info(f"Executing Perspective ingestion request with: {cmd}")

    # Do not pass the token via CLI arguments, as it may be visible in process list and
    # logs. Instead, set it as an environment variable.
    if perspective_api_token:
        os.environ["PERSPECTIVE_API_TOKEN"] = perspective_api_token

    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603
    except subprocess.CalledProcessError as err:
        msg = f"Error stderr: {err.stderr}, error output: {err.output}"
        logger.exception(msg)
        raise
