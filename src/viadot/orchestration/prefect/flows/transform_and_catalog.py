"""Build specified dbt model(s) and upload the generated metadata to Luma."""

from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Literal

from prefect import flow, task

from viadot.orchestration.prefect.tasks import (
    clone_repo,
    dbt_task,
    luma_ingest_task,
    s3_upload_file,
)
from viadot.orchestration.prefect.utils import get_credentials


@task
def remove_dbt_repo_dir(dbt_repo_dir_name: str) -> None:
    """Remove the repo directory.

    Args:
        dbt_repo_dir_name (str): The name of the dbt repo directory.
    """
    shutil.rmtree(dbt_repo_dir_name, ignore_errors=True)


@flow(
    name="Transform and Catalog",
    description="Build specified dbt model(s) and upload generated metadata to Luma.",
    timeout_seconds=2 * 60 * 60,
)
def transform_and_catalog(  # noqa: PLR0913
    dbt_repo_url: str | None = None,
    dbt_repo_url_secret: str | None = None,
    dbt_project_path: str = "dbt",
    dbt_repo_branch: str | None = None,
    dbt_repo_token_secret: str | None = None,
    dbt_selects: dict[str, str] | None = None,
    dbt_target: str | None = None,
    dbt_target_dir_path: str | None = None,
    luma_url: str = "http://localhost:8000",
    luma_follow: bool = False,
    metadata_kind: Literal["model", "model_run"] = "model_run",
    run_results_storage_path: str | None = None,
    run_results_storage_config_key: str | None = None,
    run_results_storage_credentials_secret: str | None = None,
) -> list[str]:
    """Build specified dbt model(s) and upload the generated metadata to Luma.

    Supports ingesting both model and model run metadata (controlled by the
    `metadata_kind` parameter).

    Note that metadata is still ingested even if the preceding `dbt test` task fails.
    This is done in order to capture test failure metadata in the data catalog.

    Args:
        dbt_repo_url (str, optional): The URL for cloning the dbt repo with relevant
            dbt project. Defaults to None.
        dbt_repo_url_secret (str, optional): Alternatively to above, the secret
            containing `dbt_repo_url`. Defaults to None.
        dbt_project_path (str): Path to the dbt project directory, relative to the
            dbt repository's root. For example, "dbt/my_dbt_project". Defaults to "dbt".
        dbt_repo_branch (str, optional): The branch of the dbt repo to use. Defaults to
            None (default repo branch).
        dbt_repo_token_secret (str, optional): The secret containing the personal access
            token used to clone the dbt repository, in case it's private. Not required
            if token is already included in `dbt_repo_url` (which is NOT recommended).
            Defaults to None.
        dbt_selects (dict, optional): Valid
            [dbt node selection](https://docs.getdbt.com/reference/node-selection/syntax)
            expressions. Valid keys are `run`, `test`, and `source_freshness`. The test
                select expression is taken from run's, as long as run select is
                provided. Defaults to None.
        dbt_target (str): The dbt target to use. If not specified, the default dbt
            target (as specified in `profiles.yaml`) will be used. Defaults to None.
        dbt_target_dir_path (str): The path to your dbt project's target
            directory, which contains dbt artifact JSON files, relative
            to dbt project's root directory. By default,
            `<repo_name>/<dbt_project_path>/target`, since "target" is the default
            name of the directory generated by dbt.
        luma_url (str, optional): The URL of the Luma instance to ingest into.
            Defaults to "http://localhost:8000".
        luma_follow (bool, optional): Whether to follow the ingestion process until it's
            completed (by default, ingestion request is sent without awaiting for the
            response). By default, `False`.
        metadata_kind (Literal["model", "model_run"], optional): The kind of metadata
            to ingest. Defaults to "model_run".
        run_results_storage_path (str, optional): The directory to upload the
            `run_results.json` file to. Note that a timestamp will be appended to the
            end of the file. Currently, only S3 is supported. Defaults to None.
        run_results_storage_config_key (str, optional): The key in the viadot config
            holding AWS credentials. Defaults to None.
        run_results_storage_credentials_secret (str, optional): The name of the secret
            block in Prefect holding AWS credentials. Defaults to None.

    Returns:
        list[str]: Lines from stdout of the `upload_metadata` task as a list.

    Examples:
        # Build staging models.

        ```python
        import os
        from prefect_viadot.flows import transform_and_catalog

        my_dbt_repo_url = "https://github.com/dbt-labs/jaffle_shop"
        my_luma_url = "http://localhost:8000"

        transform_and_catalog(
            dbt_repo_url=my_dbt_repo_url
            dbt_selects={"run": "staging"}
            luma_url=my_luma_url,
            run_results_storage_path="s3://my-bucket/dbt/run_results",
            run_results_storage_credentials_secret="my-aws-credentials-block",
        )
        ```

        Some common `dbt_select` patterns:
        - build a model and all its downstream dependencies: `dbt_select="my_model+"`
        - build all models in a directory: `dbt_select="models/staging"`
    """
    # Clone the dbt project.
    dbt_repo_url = dbt_repo_url or get_credentials(dbt_repo_url_secret)
    clone = clone_repo(
        url=dbt_repo_url,
        checkout_branch=dbt_repo_branch,
        token_secret=dbt_repo_token_secret,
    )

    # Prepare the environment.
    dbt_repo_name = dbt_repo_url.split("/")[-1].replace(".git", "")
    dbt_project_path_full = Path(dbt_repo_name) / dbt_project_path
    dbt_pull_deps_task = dbt_task.with_options(name="dbt_deps")
    pull_dbt_deps = dbt_pull_deps_task(
        project_path=dbt_project_path_full,
        command="deps",
        wait_for=[clone],
    )

    # Run dbt commands.
    dbt_target_option = f"-t {dbt_target}" if dbt_target is not None else ""

    if metadata_kind == "model_run":
        # Produce `run-results.json` artifact for Luma ingestion.
        if dbt_selects:
            run_select = dbt_selects.get("run")
            test_select = dbt_selects.get("test", run_select)

            run_select_safe = f"-s {run_select}" if run_select is not None else ""
            test_select_safe = f"-s {test_select}" if test_select is not None else ""
        else:
            run_select_safe = ""
            test_select_safe = ""

        run_task = dbt_task.with_options(name="dbt_run")
        run = run_task(
            project_path=dbt_project_path_full,
            command=f"run {run_select_safe} {dbt_target_option}",
            wait_for=[pull_dbt_deps],
        )

        test_task = dbt_task.with_options(name="dbt_test")
        test = test_task(
            project_path=dbt_project_path_full,
            command=f"test {test_select_safe} {dbt_target_option}",
            raise_on_failure=False,
            wait_for=[run],
        )
        upload_metadata_upstream_task = test

    else:
        # Produce `catalog.json` and `manifest.json` artifacts for Luma ingestion.
        docs_generate_task = dbt_task.with_options(name="dbt_docs_generate")
        docs = docs_generate_task(
            project_path=dbt_project_path_full,
            command="docs generate",
            wait_for=[pull_dbt_deps],
        )
        upload_metadata_upstream_task = docs

    # Upload metadata to Luma.
    if dbt_target_dir_path is None:
        dbt_target_dir_path = dbt_project_path_full / "target"

    upload_metadata = luma_ingest_task(
        metadata_kind=metadata_kind,
        metadata_dir_path=dbt_target_dir_path,
        luma_url=luma_url,
        follow=luma_follow,
        wait_for=[upload_metadata_upstream_task],
    )

    if run_results_storage_path:
        file_name = "run_results.json"
        # Add a timestamp suffix, eg. run_results_1737556947.934292.json.
        timestamp = datetime.now(timezone.utc).timestamp()
        run_results_storage_path = run_results_storage_path.rstrip("/") + "/"
        run_results_storage_path += (
            Path(file_name).stem + "_" + str(timestamp) + ".json"
        )

        # Upload the file to s3.
        dump_test_results_to_s3 = s3_upload_file(
            from_path=str(dbt_target_dir_path / file_name),
            to_path=run_results_storage_path,
            wait_for=[upload_metadata_upstream_task],
            config_key=run_results_storage_config_key,
            credentials_secret=run_results_storage_credentials_secret,
        )

    # Cleanup.
    wait_for = (
        [upload_metadata, dump_test_results_to_s3]
        if run_results_storage_path
        else [upload_metadata]
    )
    remove_dbt_repo_dir(dbt_repo_name, wait_for=wait_for)

    return remove_dbt_repo_dir
