from prefect_shell.utils import shell_run_command
from typing import List, Optional, Union
import logging
from prefect import task
from prefect.logging import get_run_logger
from pathlib import Path

dir_path = Path(__file__).resolve().parent.parent.parent
# The path to the dbt "uberproject" (containing all other projects).
dbt_uberproject_path = dir_path.joinpath("dbt")


@task
async def dbt_task(
    command: str = "run",
    env: Optional[dict] = None,
    dbt_project_name: str = "databricks",
    shell: str = "bash",
    return_all: bool = False,
    stream_level: int = logging.INFO,
) -> Union[List, str]:
    """
    Runs dbt commands within a shell.
    Args:
        command: dbt command to be executed; can also be
            provided post-initialization by calling this task instance.
        env: Dictionary of environment variables to use for
            the subprocess; can also be provided at runtime.
        dbt_project_name: The name of the dbt project to execute the command in.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string.
        stream_level: The logging level of the stream;
            defaults to 20 equivalent to `logging.INFO`.
    Returns:
        If return all, returns all lines as a list; else the last line as a string.
    Example:
        List contents in the current directory.
        ```python
        from prefect import flow
        from prefect_shell import shell_run_command
        @flow
        def example_shell_run_command_flow():
            return shell_run_command(command="ls .", return_all=True)
        example_shell_run_command_flow()
        ```
    """
    logger = get_run_logger()
    dbt_project_path = dbt_uberproject_path.joinpath(dbt_project_name)
    helper_command = f"cd {dbt_project_path}"

    result = await shell_run_command(
        command="dbt " + command,
        env=env,
        helper_command=helper_command,
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        logger=logger,
    )
    return result
