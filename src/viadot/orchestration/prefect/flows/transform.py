"""Build specified dbt model(s)."""

import os
import shutil

from prefect import flow, task

from viadot.orchestration.prefect.tasks import clone_repo, dbt_task
from viadot.orchestration.prefect.utils import get_credentials


@task
def _cleanup_repo(dbt_repo_dir_name: str) -> None:
    """Remove a repo folder.

    Args:
        dbt_repo_dir_name (str): The name of the temporary folder.
    """
    shutil.rmtree(dbt_repo_dir_name, ignore_errors=True)  # Delete folder on run


@flow(
    name="Transform",
    description="Build specified dbt model(s).",
    timeout_seconds=2 * 60 * 60,
)
def transform(
    dbt_project_path: str,
    dbt_repo_url: str | None = None,
    dbt_repo_url_secret: str | None = None,
    dbt_repo_branch: str | None = None,
    token_secret: str | None = None,
    local_dbt_repo_path: str | None = None,
    dbt_selects: dict[str, str] | None = None,
    dbt_target: str | None = None,
) -> None:
    """Build specified dbt model(s).

    This flow implements a simplified version of the `transform_and_catalog()` flow,
    excluding metadata handling, source freshness checks, and stateful operations.

    Args:
        dbt_project_path (str): The path to the dbt project (the directory containing
            the `dbt_project.yml` file).
        dbt_repo_url (str, optional): The URL for cloning the dbt repo with relevant
            dbt project.
        dbt_repo_url_secret (str, optional): Alternatively to above, the secret
            containing `dbt_repo_url`.
        dbt_repo_branch (str, optional): The branch of the dbt repo to use.
        token_secret (str, optional): The name of the secret storing the git token.
            Defaults to None.
        local_dbt_repo_path (str, optional): The path where to clone the repo to.
        dbt_selects (dict, optional): Valid
            [dbt node selection](https://docs.getdbt.com/reference/node-selection/syntax)
            expressions. Valid keys are `run`, `test`, and `source_freshness`. The test
                select expression is taken from run's, as long as run select is
                provided. Defaults to None.
        dbt_target (str): The dbt target to use. If not specified, the default dbt
            target (as specified in `profiles.yaml`) will be used. Defaults to None.

    Returns:
        list[str]: Lines from stdout of the `upload_metadata` task as a list.

    Examples:
        # Build a single model
        ```python
        import os
        from prefect_viadot.flows import transform_and_catalog

        my_dbt_project_path = os.path.expanduser("~/dbt/my_dbt_project")

        transform_and_catalog(
            dbt_project_path=my_dbt_project_path,
            dbt_selects={"run": "my_model"}
        )
        ```

        Some common `dbt_select` patterns:
        - build a model and all its downstream dependencies: `dbt_select="my_model+"`
        - build all models in a directory: `dbt_select="models/my_project"`
        ```
    """
    dbt_repo_url = dbt_repo_url or get_credentials(dbt_repo_url_secret)
    local_dbt_repo_path = (
        os.path.expandvars(local_dbt_repo_path)
        if local_dbt_repo_path is not None
        else "tmp_dbt_repo_dir"
    )

    clone = clone_repo(
        url=dbt_repo_url,
        checkout_branch=dbt_repo_branch,
        token_secret=token_secret,
        path=local_dbt_repo_path,
    )

    # dbt CLI does not handle passing --target=None
    dbt_target_option = f"-t {dbt_target}" if dbt_target is not None else ""

    # Clean up artifacts from previous runs (`target/` dir and packages)
    dbt_clean_task = dbt_task.with_options(name="dbt_task_clean")
    dbt_clean_up = dbt_clean_task(
        project_path=dbt_project_path, command="clean", wait_for=[clone]
    )
    dbt_pull_deps_task = dbt_task.with_options(name="dbt_task_deps")
    pull_dbt_deps = dbt_pull_deps_task(
        project_path=dbt_project_path,
        command="deps",
        wait_for=[dbt_clean_up],
    )

    run_select = dbt_selects.get("run")
    run_select_safe = f"-s {run_select}" if run_select is not None else ""
    run = dbt_task(
        project_path=dbt_project_path,
        command=f"run {run_select_safe} {dbt_target_option}",
        wait_for=[pull_dbt_deps],
    )

    test_select = dbt_selects.get("test", run_select)
    test_select_safe = f"-s {test_select}" if test_select is not None else ""
    test = dbt_task(
        project_path=dbt_project_path,
        command=f"test {test_select_safe} {dbt_target_option}",
        wait_for=[run],
    )

    _cleanup_repo(
        local_dbt_repo_path,
        wait_for=[test],
    )
