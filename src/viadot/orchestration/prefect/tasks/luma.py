"""Tasks for interacting with [Luma](https://github.com/dyvenia/luma)."""

import logging
import os
from pathlib import Path
from typing import Any, Literal

from prefect import task
from prefect.logging import get_run_logger

from viadot.orchestration.prefect.utils import shell_run_command


@task(retries=2, retry_delay_seconds=5, timeout_seconds=60 * 10)
async def luma_ingest_task(  # noqa: PLR0913
    metadata_dir_path: str | Path,
    luma_url: str = "http://localhost:8000",
    metadata_kind: Literal["model", "model_run"] = "model",
    follow: bool = False,
    env: dict[str, Any] | None = None,
    shell: str = "bash",
    return_all: bool = True,
    stream_level: int = logging.INFO,
    max_retries: int = 3,  # noqa: ARG001
    logger: logging.Logger | None = None,
    raise_on_failure: bool = True,
) -> list[str]:
    """Runs Luma ingestion by sending dbt artifacts to Luma ingestion API.

    Args:
        metadata_dir_path: The path to the directory containing metadata files.
            In the case of dbt, it's dbt project's `target` directory,
            which contains dbt artifacts (`sources.json`, `catalog.json`,
            `manifest.json`, and `run_results.json`).
        luma_url: The URL of the Luma instance to ingest into.
        metadata_kind: The kind of metadata to ingest. Either `model` or `model_run`.
        follow: Whether to follow the ingestion process until it's completed (by
            default, ingestion request is sent without awaiting for the response). By
            default, `False`.
        env: Dictionary of environment variables to use for
            the subprocess; can also be provided at runtime.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string.
        stream_level: The logging level of the stream;
            defaults to 20; equivalent to `logging.INFO`.
        max_retries: The maximum number of times to retry the task. Defaults to 3.
        logger: The logger to use for logging the task's output. By default, the
            Prefect's task run logger.
        raise_on_failure: Whether to raise an exception if the command fails.

    Returns:
        list[str]: Lines from stdout as a list.

    Example:
        ```python
        from prefect import flow
        from viadot.tasks import luma_ingest_task

        metadata_dir_path = "${HOME}/dbt/my_dbt_project/target"

        @flow
        def example_luma_ingest_flow():
            return luma_ingest_task(metadata_dir_path=metadata_dir_path)

        example_luma_ingest_flow()
        ```
    """
    if not logger:
        logger = get_run_logger()

    if isinstance(metadata_dir_path, str):
        path_expanded = os.path.expandvars(metadata_dir_path)
        metadata_dir_path = Path(path_expanded)

    luma_command = "ingest" if metadata_kind == "model" else "send-test-results"
    follow_flag = "--follow" if follow else ""
    command = (
        f"luma dbt {luma_command} -m {metadata_dir_path} -l {luma_url} {follow_flag}"
    )

    return await shell_run_command(
        command=command,
        env=env,
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        logger=logger,
        raise_on_failure=raise_on_failure,
    )
