import os

from viadot.tasks.github import DownloadGitHubFile


def test_download_github_file():
    task = DownloadGitHubFile()
    task.run(repo="dyvenia/viadot", from_path="LICENSE")
    assert os.path.exists("LICENSE")
    os.remove("LICENSE")
