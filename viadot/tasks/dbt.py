from prefect_shell.utils import shell_run_command
from typing import List, Optional, Union
import logging
from prefect import task
from prefect.logging import get_run_logger


@task
async def dbt_task(
    command: str = "run",
    project_path: str = None,
    env: Optional[dict] = None,
    shell: str = "bash",
    return_all: bool = False,
    stream_level: int = logging.INFO,
) -> Union[List, str]:
    """
    Runs dbt commands within a shell.
    Args:
        command: dbt command to be executed; can also be
            provided post-initialization by calling this task instance.
        project_path: The path to the dbt project.
        env: Dictionary of environment variables to use for
            the subprocess; can also be provided at runtime.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string.
        stream_level: The logging level of the stream;
            defaults to 20 equivalent to `logging.INFO`.
    Returns:
        If return all, returns all lines as a list; else the last line as a string.
    Example:

        Executes `dbt run` on a specified dbt project.

        ```python
        from prefect import flow
        from viadot.tasks import dbt_task

        PROJECT_PATH = "/home/viadot/dbt/my_dbt_project"

        @flow
        def example_dbt_task_flow():
            return dbt_task(
                command="run", project_path=PROJECT_PATH, return_all=True
            )

        example_dbt_task_flow()
        ```
    """
    logger = get_run_logger()

    result = await shell_run_command(
        command=f"dbt {command}",
        env=env,
        helper_command=f"cd {project_path}",
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        logger=logger,
    )
    return result
