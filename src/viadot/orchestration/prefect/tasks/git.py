"""Tasks for interacting with git repositories."""

import logging
import shutil
from typing import Any

from prefect import get_run_logger, task
import pygit2

from viadot.orchestration.prefect.utils import get_credentials


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 10)
def clone_repo(
    url: str,
    token: str | None = None,
    token_secret: str | None = None,
    logger: logging.Logger | None = None,
    **kwargs: dict[str, Any] | None,
) -> None:
    """Clone Azure DevOps or GitHub repository.

    Args:
        url (str): Alternatively to URL + token or URL + token_secret, you can also
            provide the full URL (including token, if the repo is private) here.
        token (str, optional): The token to use to clone the repo.
        token_secret (str, optional): The secret holding the token.
        logger (logging.Logger): The logger to use. By default, Prefect's task run
            logger is used.
        token (str, optional): The personal access token.
        token_secret (str, optional): The name of the secret storing the token. Defaults
            to None.
        logger (logging.Logger, optional): The logger to use for logging the task's
            output. By default, Prefect's task run logger.
        kwargs (dict): Keyword arguments to be passed to `pygit2.clone_repository()`.
        **kwargs (dict, optional): Keyword arguments to pass to
            `pygit2.clone_repository()`.

    Examples:
        Azure DevOps (public repo):
        https://dev.azure.com/{organization_name}/{project_name}/_git/{repo_name}
        Azure DevOps (private repo):
        https://{token}@dev.azure.com/{organization_name}/{project_name}/_git/{repo_name}
        GitHub (public repo): https://github.com/{organization_name}/{repo_name}.git
        GitHub (private repo): https://{token}@github.com/{organization_name}/{repo_name}.git
    """
    if not logger:
        logger = get_run_logger()

    url = url.strip("/")

    if token_secret:
        token = get_credentials(token_secret)

    if token:
        url = url.replace("https://dev.azure.com", f"https://{token}@dev.azure.com")
        url = url.replace("https://github.com", f"https://{token}@github.com")
        url = url.replace("gitlab", f"oauth2:{token}@gitlab")

    repo_name = url.split("/")[-1].replace(".git", "")
    path = kwargs.get("path") or repo_name
    kwargs["path"] = path

    logger.info(f"Removing {path}...")
    shutil.rmtree(path, ignore_errors=True)  # Delete folder on run

    logger.info(f"Cloning repo '{repo_name}' into {path}...")
    pygit2.clone_repository(url, **kwargs)
    logger.info(f"Repo '{repo_name}' has been successfully cloned into {path}.")
