"""Tasks for interacting with [DataHub](https://datahubproject.io/)."""

import logging
import os
from pathlib import Path
from typing import Any

from orchestration.prefect_viadot.utils import shell_run_command
from prefect import task
from prefect.logging import get_run_logger


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
async def datahub_ingest_task(
    recipe_path: str | Path,
    env: dict[str, Any] | None = None,
    debug: bool = False,
    shell: str = "bash",
    return_all: bool = True,
    stream_level: int = logging.INFO,
    max_retries: int = 3,  # noqa: ARG001
) -> list[str]:
    """Runs DataHub ingestion.

    Sends the provided recipe to DataHub ingestion REST API. We do this by first
    changing the directory to the one containing the recipe and then running
    `datahub ingest`. This way, using relative paths inside the recipe file is intuitive
    (you always put in paths relative to the file).

    Args:
        recipe_path: The path to the recipe YAML for the ingestion.
        env: Dictionary of environment variables to use for the subprocess; can also be
            provided at runtime.
        debug (bool): Whether to print debug logs. Defaults to False.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string.
        stream_level: The logging level of the stream; defaults to 20, equivalent to
            `logging.INFO`.
        max_retries (int): The maximum number of times to retry the task. Defaults to 3.

    Returns:
        list[str]: Lines from stdout as a list.

    Example:
        Ingests a dbt recipe.
        ```python
        from prefect import flow
        from viadot.tasks import datahub_ingest_task

        RECIPE_PATH = "/home/viadot/catalog/recipes/dbt_run_recipe.yml"

        @flow
        def example_datahub_ingest_flow():
            return datahub_ingest_task(recipe_path=RECIPE_PATH, return_all=True)

        example_datahub_ingest_flow()
        ```
    """
    logger = get_run_logger()

    if isinstance(recipe_path, str):
        path_expanded = os.path.expandvars(recipe_path)
        recipe_path = Path(path_expanded)

    recipes_path = Path(recipe_path).parent
    debug_flag = "--debug" if debug else ""

    return await shell_run_command(
        command=f"datahub {debug_flag} ingest -c {recipe_path.name}",
        helper_command=f"cd {recipes_path}",
        env=env,
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        logger=logger,
    )


@task
async def datahub_cleanup_task(
    env: dict[str, Any] | None = None,
    shell: str = "bash",
    return_all: bool = False,
    stream_level: int = logging.INFO,
    max_retries: int = 3,  # noqa: ARG001
) -> list[str] | str:
    """Remove all metadata from a DataHub instance.

    Args:
        env: Dictionary of environment variables to use for the subprocess; can also be
            provided at runtime.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string.
        stream_level: The logging level of the stream; defaults to 20, equivalent to
            `logging.INFO`.
        max_retries (int): The maximum number of times to retry the task. Defaults to 3.

    Returns:
        If return_all, returns all lines as a list; else the last line as a string.

    Example:
        ```python
        from prefect import flow
        from viadot.tasks import datahub_cleanup_task
        @flow
        def example_datahub_cleanup_flow():
            return datahub_cleanup_task(return_all=True)
        example_datahub_cleanup_flow()
        ```
    """
    logger = get_run_logger()

    delete_env = "datahub delete -f --hard -e dev"
    delete_tests = "datahub delete -f --hard -p dbt --entity_type assertion"

    return await shell_run_command(
        command=f"{delete_env} && {delete_tests}",
        env=env,
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        logger=logger,
    )
