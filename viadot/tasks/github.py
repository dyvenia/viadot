import base64

# import shutil
import urllib

# from typing import Any

# import pygit2
from prefect import Task
from prefect.client import Secret
from prefect.utilities.tasks import defaults_from_attrs

import os

from github import Github


# This task is broken on some systems
# class CloneRepo(Task):
#     """
#     Task for cloning a GitHub repository.

#     Args:
#     - repo (str, optional): the name of the repository to clone; must be
#         provided in the form `organization/repo_name` or `user/repo_name`; can also be
#         provided to the `run` method
#     - to (str, optional): the destination folder for the repository; defaults to the repository's name
#     - bare (bool, optional): whether to clone a read-only copy; defaults to `False`

#     Example:
#         ```python
#         from prefect import Flow
#         from viadot.tasks.github import CloneRepo
#         with Flow(name="example") as f:
#             task = CloneRepo()(repo='fishtown-analytics/dbt')
#         out = f.run()
#         ```
#     """

#     def __init__(
#         self,
#         repo: str = None,
#         to: str = None,
#         bare: bool = False,
#         access_token_secret: str = "github_token",
#         **kwargs: Any,
#     ):
#         self.repo = repo
#         self.to = to or repo.split("/")[-1] if repo else to
#         self.bare = bare
#         self.access_token_secret = access_token_secret
#         super().__init__(**kwargs)

#     @defaults_from_attrs("repo", "to", "bare", "access_token_secret")
#     def run(
#         self,
#         repo: str = None,
#         to: str = None,
#         bare: bool = None,
#         access_token_secret: str = None,
#     ):
#         """
#         Clones the repo.

#         Args:
#         - repo (str, optional): the name of the repository to clone; must be
#             provided in the form `organization/repo_name` or `user/repo_name`
#         - to (str, optional): the destination folder for the repository; defaults to the repository's name
#         - bare (bool, optional): whether to clone a read-only copy; defaults to `False`
#         """

#         shutil.rmtree(to, ignore_errors=True)  # Delete folder on run

#         git_token = Secret(access_token_secret).get()
#         repo_url = f"https://{git_token}:x-oauth-basic@github.com/{repo}"

#         if os.path.dirname(to):
#             os.makedirs(os.path.dirname(to), exist_ok=True)
#         pygit2.clone_repository(repo_url, to, bare=bare)

#         self.logger.info(f"Repo {repo} has been successfully cloned.")


class DownloadGitHubFile(Task):
    """
    Task for downloading a file from GitHub.

    Args:
        repo (str, optional): The repository in the format `org/repo`. Defaults to None.
        from_path (str, optional): The path to the file. Defaults to None.
        to_path (str, optional): The destination path. Defaults to None.
        access_token_secret (str, optional): The Prefect secret containing GitHub token. Defaults to "github_token".
        branch (str, optional): The GitHub branch to use. Defaults to "main".
    """

    def __init__(
        self,
        repo: str = None,
        from_path: str = None,
        to_path: str = None,
        access_token_secret: str = "github_token",
        branch: str = "main",
        **kwargs,
    ):
        self.repo = repo
        self.from_path = from_path
        self.to_path = to_path
        self.access_token_secret = access_token_secret
        self.branch = branch
        super().__init__(**kwargs)

    @defaults_from_attrs(
        "repo", "from_path", "to_path", "access_token_secret", "branch"
    )
    def run(
        self,
        repo: str = None,
        from_path: str = None,
        to_path: str = None,
        access_token_secret: str = None,
        branch: str = None,
    ):
        """Task run method.

        Args:
            repo (str, optional): The repository in the format `org/repo`. Defaults to None.
            from_path (str, optional): The path to the file. Defaults to None.
            to_path (str, optional): The destination path. Defaults to None.
            access_token_secret (str, optional): The Prefect secret containing GitHub token. Defaults to "github_token".
            branch (str, optional): The GitHub branch to use. Defaults to "main".
        """
        git_token = Secret(access_token_secret).get()
        repo_name = repo.split("/")[-1]
        to_path = to_path or os.path.join(repo_name, from_path)

        g = Github(git_token)
        repo = g.get_repo(repo)
        content_encoded = repo.get_contents(
            urllib.parse.quote(from_path), ref=branch
        ).content
        content = base64.b64decode(content_encoded)

        if os.path.dirname(to_path):
            os.makedirs(os.path.dirname(to_path), exist_ok=True)
        with open(to_path, "w") as f:
            f.write(content.decode())
