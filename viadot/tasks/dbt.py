import logging
import os
from typing import Any, Dict, List, Optional, Union


from prefect import Task
from prefect.utilities.logging import get_logger
from prefect.utilities.tasks import defaults_from_attrs

from viadot.utils import shell_run_command


class DBTTask(Task):
    """Tasks for interacting with [dbt](https://www.getdbt.com/)."""

    def __init__(
        self,
        name: str = "dbt_task",
        command: str = "run",
        project_path: str = None,
        env: Optional[dict] = None,
        shell: str = "bash",
        return_all: bool = False,
        stream_level: int = logging.INFO,
        timeout: int = 60 * 60,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        Runs dbt commands within a shell.

        Args:
            name(str, optional): The name of the task. Defaults to "dbt_task".
            command (str, optional): dbt command to be executed; can also be provided post-initialization by calling this task instance.
                Defaults to "run".
            project_path (str, optional): The path to the dbt project. Defaults to None.
            env (Optional[dict], optional): Dictionary of environment variables to use for the subprocess; can also be provided at runtime.
                Defaults to None.
            shell (str, optional): Shell to run the command with. Defaults to "bash".
            return_all (bool, optional): Whether this task should return all lines of stdout as a list, or just the last line as a string.
                Defaults to False.
            stream_level (int, optional): The logging level of the stream; Defaults to 20 equivalent to `logging.INFO`.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before a timeout occurs. Defaults to 600.
        """
        self.name = name
        self.command = command
        self.project_path = project_path
        self.env = env
        self.shell = shell
        self.return_all = return_all
        self.stream_level = stream_level

        super().__init__(
            name=self.name,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Run DBT within a shell."""
        super().__call__(self)

    @defaults_from_attrs(
        "command", "project_path", "env", "shell", "return_all", "stream_level"
    )
    async def run(
        self,
        command: str = "run",
        project_path: str = None,
        env: Optional[dict] = None,
        shell: str = "bash",
        return_all: bool = False,
        stream_level: int = logging.INFO,
        **kwargs: Dict[str, Any],
    ) -> Union[List, str]:
        """Run methon of DBTTask task.

        Args:
            command (str, optional): dbt command to be executed; can also be provided post-initialization by calling this task instance.
                Defaults to "run".
            project_path (str, optional): The path to the dbt project. Defaults to None.
            env (Optional[dict], optional): Dictionary of environment variables to use for the subprocess; can also be provided at runtime.
                Defaults to None.
            shell (str, optional): Shell to run the command with. Defaults to "bash".
            return_all (bool, optional): Whether this task should return all lines of stdout as a list, or just the last line as a string.
                Defaults to False.
            stream_level (int, optional): The logging level of the stream; Defaults to 20 equivalent to `logging.INFO`.

        Returns:
            Union: If return all, returns all lines as a list; else the last line as a string.
        """
        logger = get_logger(__name__)

        project_path = (
            os.path.expandvars(project_path) if project_path is not None else "."
        )

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
