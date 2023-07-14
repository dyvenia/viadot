import shutil
from typing import Dict

import pygit2
from prefect.utilities.logging import get_logger
from prefect.utilities.tasks import defaults_from_attrs

from viadot.task_utils import *


class CloneRepo(Task):
    """Tasks for interacting with git repositories."""

    def __init__(
        self,
        url: str,
        token: str = None,
        token_secret: str = None,
        timeout: int = 60 * 10,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        Clone Azure DevOps or GitHub repository.

        Args:
            url (str): Alternatively to all above, you can also provide the full URL (including token, if the repo is private) here.
                Examples:
                    Azure DevOps (public repo): https://dev.azure.com/{organization_name}/{project_name}/_git/{repo_name}  # noqa
                    Azure DevOps (private repo): https://{token}@dev.azure.com/{organization_name}/{project_name}/_git/{repo_name}  # noqa
                    GitHub (public repo): https://github.com/{organization_name}/{repo_name}.git  # noqa
                    GitHub (private repo): https://{token}@github.com/{organization_name}/{repo_name}.git  # noqa
            token (str, optional): The personal access token. Defaults to None.
            token_secret (str, optional): The name of the secret storing the token. Defaults to None.
                Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before a timeout occurs.
                Defaults to 600.
            kwargs (dict): Keyword arguments to be passed to `pygit2.clone_repository()`.
        """

        self.url = url
        self.token = token
        self.token_secret = token_secret

        super().__init__(
            name="clone_repo",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Clone repository"""
        super().__call__(self)

    @defaults_from_attrs("url", "token", "token_secret")
    def run(
        self,
        url: str,
        token: str = None,
        token_secret: str = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Run methon of CloneRepo task.

        Args:
            url (str): Alternatively to all above, you can also provide the full URL (including token, if the repo is private) here.
                Examples:
                    Azure DevOps (public repo): https://dev.azure.com/{organization_name}/{project_name}/_git/{repo_name}  # noqa
                    Azure DevOps (private repo): https://{token}@dev.azure.com/{organization_name}/{project_name}/_git/{repo_name}  # noqa
                    GitHub (public repo): https://github.com/{organization_name}/{repo_name}.git  # noqa
                    GitHub (private repo): https://{token}@github.com/{organization_name}/{repo_name}.git  # noqa
            token (str, optional): The personal access token. Defaults to None.
            token_secret (str, optional): The name of the secret storing the token. Defaults to None.
                Defaults to None.
        """
        logger = get_logger(__name__)

        url = url.strip("/")

        azure_secret_task = AzureKeyVaultSecret()
        token = token or azure_secret_task.run(secret=token_secret)
        if token:
            url = url.replace("https://dev.azure.com", f"https://{token}@dev.azure.com")
            url = url.replace("https://github.com", f"https://{token}@github.com")

        repo_name = url.split("/")[-1].replace(".git", "")
        path = kwargs.get("path") or repo_name
        kwargs["path"] = path

        logger.info(f"Removing {path}...")
        shutil.rmtree(path, ignore_errors=True)  # Delete folder on run

        logger.info(f"Cloning repo '{repo_name}' into {path}...")
        pygit2.clone_repository(url, **kwargs)
        logger.info(f"Repo '{repo_name}' has been successfully cloned into {path}.")
