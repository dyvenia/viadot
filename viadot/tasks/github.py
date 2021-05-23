import shutil
from typing import Any

import pygit2
from prefect import Task
from prefect.client import Secret
from prefect.utilities.tasks import defaults_from_attrs


class CloneRepo(Task):
    """
    Task for cloning a GitHub repository.

    Args:
    - repo (str, optional): the name of the repository to clone; must be
        provided in the form `organization/repo_name` or `user/repo_name`; can also be
        provided to the `run` method
    - to (str, optional): the destination folder for the repository; defaults to the repository's name
    - bare (bool, optional): whether to clone a read-only copy; defaults to `False`

    Example:
        ```python
        from prefect import Flow
        from viadot.tasks.github import CloneRepo
        with Flow(name="example") as f:
            task = CloneRepo()(repo='fishtown-analytics/dbt')
        out = f.run()
        ```
    """

    def __init__(
        self,
        repo: str = None,
        to: str = None,
        bare: bool = False,
        access_token_secret: str = "github_token",
        **kwargs: Any,
    ):
        self.repo = repo
        self.to = to or repo.split("/")[-1] if repo else to
        self.bare = bare
        self.access_token_secret = access_token_secret
        super().__init__(**kwargs)

    @defaults_from_attrs("repo", "to", "bare", "access_token_secret")
    def run(
        self,
        repo: str = None,
        to: str = None,
        bare: bool = None,
        access_token_secret: str = None,
    ):
        """
        Clones the repo.

        Args:
        - repo (str, optional): the name of the repository to clone; must be
            provided in the form `organization/repo_name` or `user/repo_name`
        - to (str, optional): the destination folder for the repository; defaults to the repository's name
        - bare (bool, optional): whether to clone a read-only copy; defaults to `False`
        """

        shutil.rmtree(to, ignore_errors=True)  # Delete folder on run

        git_token = Secret(access_token_secret).get()
        repo_url = f"https://{git_token}:x-oauth-basic@github.com/{repo}"
        pygit2.clone_repository(repo_url, to, bare=bare)

        self.logger.info(f"Repo {repo} has been successfully cloned.")
