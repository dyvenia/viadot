import shutil
from pathlib import Path

from loguru import logger
from orchestration.prefect_viadot.tasks import clone_repo


def test_clone_repo_private(AZURE_REPO_URL):
    test_repo_dir = "test_repo_dir"

    assert not Path(test_repo_dir).exists()

    clone_repo.fn(url=AZURE_REPO_URL, path=test_repo_dir, logger=logger)

    assert Path(test_repo_dir).exists()

    shutil.rmtree(test_repo_dir)
