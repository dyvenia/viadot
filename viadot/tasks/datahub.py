from prefect_shell.utils import shell_run_command
from typing import List, Optional, Union
import logging
from prefect import task
from prefect.logging import get_run_logger
from pathlib import Path


@task
async def datahub_ingest_task(
    recipe_path: Union[str, Path],
    env: Optional[dict] = None,
    shell: str = "bash",
    return_all: bool = False,
    stream_level: int = logging.INFO,
    max_retries: int = 3,
) -> Union[List, str]:
    """
    Runs DataHub ingestion by sending the provided recipe to
    DataHub ingestion REST API.

    We do this by first changing the directory to the one containing the recipe and
    then running `datahub ingest`. This way, using relative paths inside the recipe
    file is intuitive (you always put in paths relative to the file).

    Args:
        recipe_path: The path to the recipe YAML for the ingestion.
        env: Dictionary of environment variables to use for
            the subprocess; can also be provided at runtime.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string.
        stream_level: The logging level of the stream;
            defaults to 20 equivalent to `logging.INFO`.
    Returns:
        If return_all, returns all lines as a list; else the last line as a string.
    Example:
        Igests a dbt recipe.

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
        recipe_path = Path(recipe_path)

    recipes_path = Path(recipe_path).parent

    result = await shell_run_command(
        command=f"datahub ingest -c {recipe_path.name}",
        helper_command=f"cd {recipes_path}",
        env=env,
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        logger=logger,
    )
    return result
