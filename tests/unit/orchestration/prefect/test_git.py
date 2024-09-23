from pathlib import Path
import shutil

from loguru import logger

from viadot.orchestration.prefect.tasks import clone_repo


def test_clone_repo_public():
    test_repo_dir = "test_repo_dir"

    assert not Path(test_repo_dir).exists()

    clone_repo.fn(
        url="https://github.com/dbt-labs/dbt-starter-project.git",
        path=test_repo_dir,
        logger=logger,
    )

    assert Path(test_repo_dir).exists()

    shutil.rmtree(test_repo_dir)
