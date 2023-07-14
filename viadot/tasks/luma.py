import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from prefect import Task
from prefect.utilities.logging import get_logger
from prefect.utilities.tasks import defaults_from_attrs

from ..utils import shell_run_command


class LumaIngest(Task):
    """Tasks for interacting with [Luma](https://github.com/dyvenia/luma)."""

    def __init__(
        self,
        metadata_dir_path: Union[str, Path],
        endpoint: str = "http://localhost/api/v1/dbt",
        env: Optional[dict] = None,
        shell: str = "bash",
        return_all: bool = True,
        stream_level: int = logging.INFO,
        max_retries: int = 3,
        timeout: int = 60 * 10,
        retry_delay: str = 5,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        Runs Luma ingestion by sending dbt artifacts to Luma ingestion API.

        Args:
            metadata_dir_path (Union[str, Path]): The path to the directory containing metadata files.
                In the case of dbt, it's dbt project's `target` directory, which contains dbt artifacts
                (`sources.json`, `catalog.json`, `manifest.json`, and `run_results.json`).
            endpoint (str, optional): The endpoint of the Luma ingestion API. Defaults to "http://localhost/api/v1/dbt".
            env (Optional[dict], optional): Dictionary of environment variables to use for the subprocess; can also
                be provided at runtime. Defaults to None.
            shell (str, optional): Shell to run the command with. Defaults to "bash".
            return_all (bool, optional): Whether this task should return all lines of stdout as a list,
                or just the last line as a string. Defaults to True.
            stream_level (int, optional): The logging level of the stream. Defaults to 20; equivalent to `logging.INFO`.
            max_retries (int, optional): Maximum number of retries. Defaults to 3.
            timeout (Dict[str, Any], optional): The amount of time (in seconds) to wait while running this task before a timeout occurs.
                Defaults to 600.
        """
        self.metadata_dir_path = metadata_dir_path
        self.endpoint = endpoint
        self.env = env
        self.shell = shell
        self.return_all = return_all
        self.stream_level = stream_level
        self.max_retries = max_retries

        super().__init__(
            name="luma_ingest",
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Ingest data to Luma."""
        super().__call__(self)

    @defaults_from_attrs(
        "metadata_dir_path", "endpoint", "env", "shell", "return_all", "stream_level"
    )
    async def run(
        self,
        metadata_dir_path: Union[str, Path] = None,
        endpoint: str = "http://localhost/api/v1/dbt",
        env: Optional[dict] = None,
        shell: str = "bash",
        return_all: bool = True,
        stream_level: int = logging.INFO,
    ):
        """
        Run methon of LumaIngest task.

        Args:
            metadata_dir_path (Union[str, Path]): The path to the directory containing metadata files.
                In the case of dbt, it's dbt project's `target` directory, which contains dbt artifacts
                (`sources.json`, `catalog.json`, `manifest.json`, and `run_results.json`). Defaults to None.
            endpoint (str, optional): The endpoint of the Luma ingestion API. Defaults to "http://localhost/api/v1/dbt"
            env (Optional[dict], optional): Dictionary of environment variables to use for the subprocess; can also
                be provided at runtime. Defaults to None.
            shell (str, optional): Shell to run the command with. Defaults to "bash".
            return_all (bool, optional): Whether this task should return all lines of stdout as a list,
                or just the last line as a string. Defaults to True.
            stream_level (int, optional): The logging level of the stream. Defaults to 20; equivalent to `logging.INFO`.

        Returns:
            List[str]: Lines from stdout as a list.
        """

        logger = get_logger()

        if isinstance(self.metadata_dir_path, str):
            path_expanded = os.path.expandvars(metadata_dir_path)
            metadata_dir_path = Path(path_expanded)

        result = await shell_run_command(
            command=f"luma dbt ingest {metadata_dir_path} -e {endpoint}",
            env=env,
            shell=shell,
            return_all=return_all,
            stream_level=stream_level,
            logger=logger,
        )
        return result
