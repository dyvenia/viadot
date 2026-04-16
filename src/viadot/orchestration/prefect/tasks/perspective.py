"""Tasks for interacting with [Perspective](https://github.com/dyvenia/perspective)."""

import logging
import os
from pathlib import Path
from typing import Any

from prefect import task
from prefect.logging import get_run_logger

from viadot.orchestration.prefect.utils import shell_run_command


@task(retries=2, retry_delay_seconds=5, timeout_seconds=60 * 10)
async def perspective_ingest_task(
    metadata_dir_path: str | Path,
    perspective_url: str = "http://localhost:8000",
    follow: bool = False,
    env: dict[str, Any] | None = None,
    shell: str = "bash",
    return_all: bool = True,
    stream_level: int = logging.INFO,
    logger: logging.Logger | None = None,
    raise_on_failure: bool = True,
) -> str | list[str]:
    """Runs Perspective ingestion by sending dbt artifacts to Perspective ingestion API.

    Args:
        metadata_dir_path: The path to the directory containing metadata files.
            In the case of dbt, it's dbt project's `target` directory,
            which contains dbt artifacts (`sources.json`, `catalog.json`,
            `manifest.json`, and `run_results.json`).
        perspective_url: The URL of the Perspective instance to ingest into.
        follow: Whether to follow the ingestion process until it's completed (by
            default, ingestion request is sent without awaiting for the response). By
            default, `False`.
        env: Dictionary of environment variables to use for the subprocess; can also be
            provided at runtime.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string.
        stream_level: The logging level of the stream;
            defaults to 20; equivalent to `logging.INFO`.
        logger: The logger to use for logging the task's output. By default, the
            Prefect's task run logger.
        raise_on_failure: Whether to raise an exception if the command fails.

    Returns:
        str | list[str]: If `return_all` is True, returns all lines as a list; else the
            last line as a string.

    Example:
        ```python
        from prefect import flow
        from viadot.tasks import perspective_ingest_task

        metadata_dir_path = "${HOME}/dbt/my_dbt_project/target"

        @flow
        def example_perspective_ingest_flow():
            return perspective_ingest_task(metadata_dir_path=metadata_dir_path)

        example_perspective_ingest_flow()
        ```
    """
    if not logger:
        logger = get_run_logger()

    if isinstance(metadata_dir_path, str):
        path_expanded = os.path.expandvars(metadata_dir_path)
        metadata_dir_path = Path(path_expanded)

    follow_flag = "--follow" if follow else ""
    command = f"perspective ingest dbt --test-results -m {metadata_dir_path} -u {perspective_url} {follow_flag}"

    return await shell_run_command(
        command=command,
        env=env,
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        logger=logger,
        raise_on_failure=raise_on_failure,
    )
