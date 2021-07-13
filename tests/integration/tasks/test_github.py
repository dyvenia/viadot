import os

from viadot.tasks.github import DownloadGitHubFile

# def test_github_clone_task():
#     clone_repo_task = CloneRepo()
#     repo = "fishtown-analytics/dbt"
#     repo_name = repo.split("/")[-1]
#     clone_repo_task.run(repo=repo)
#     assert os.path.exists(repo_name)


def test_download_github_file():
    task = DownloadGitHubFile()
    task.run(repo="dyvenia/viadot", from_path="LICENSE")
    assert os.path.exists("LICENSE")
    os.remove("LICENSE")
