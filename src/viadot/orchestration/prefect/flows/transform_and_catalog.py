"""Build specified dbt model(s) and upload the generated metadata to Luma."""

from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
from typing import Literal

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.states import State

from viadot.orchestration.prefect.tasks import (
    clone_repo,
    dbt_task,
    dbt_test_failure_notifier,
    luma_ingest_task,
    perspective_ingest_task,
    s3_upload_file,
)
from viadot.orchestration.prefect.tasks.dbt import (
    trigger_downstream_node,
    update_node_state,
)
from viadot.orchestration.prefect.utils import (
    DEFAULT_TIMEOUT_SECONDS,
    get_credentials,
    with_flow_timeout_param,
)


@task(cache_policy=None)
def remove_dbt_repo_dir(dbt_repo_dir_name: str) -> None:
    """Remove the repo directory.

    Args:
        dbt_repo_dir_name (str): The name of the dbt repo directory.
    """
    shutil.rmtree(dbt_repo_dir_name, ignore_errors=True)


def _run_dbt_transforms(
    metadata_kind: str,
    dbt_selects: dict[str, str] | None,
    dbt_project_path_full: Path,
    dbt_target: str | None,
    fail_flow_only_on_build_failure: bool,
    timeout_seconds: int,
) -> None:
    """Execute dbt transform commands (run/test/build/docs generate).

    Raises:
        RuntimeError: If ``fail_flow_only_on_build_failure`` is True and dbt build
            produced model errors.
    """
    dbt_target_option = f"-t {dbt_target}" if dbt_target is not None else ""

    if metadata_kind == "model_run":
        # Produce `run-results.json` artifact for Luma ingestion.
        build_select = None
        run_select_safe = ""
        test_select_safe = ""
        if dbt_selects:
            build_select = dbt_selects.get("build")
            run_select = dbt_selects.get("run")
            test_select = dbt_selects.get("test", run_select)

            build_select_safe = f"-s {build_select}" if build_select is not None else ""
            run_select_safe = f"-s {run_select}" if run_select is not None else ""
            test_select_safe = f"-s {test_select}" if test_select is not None else ""
        if build_select:
            # If build task is used, run and test tasks are not needed.
            # Build task executes run and tests commands internally.
            build_task = dbt_task.with_options(
                name="dbt_build", timeout_seconds=timeout_seconds
            )
            raise_on_failure = not fail_flow_only_on_build_failure

            build = build_task.submit(
                project_path=dbt_project_path_full,
                command=f"build {build_select_safe} {dbt_target_option}",
                raise_on_failure=raise_on_failure,
                return_all=True,
            )
            build.result()

            if fail_flow_only_on_build_failure:
                build_result = build.result()
                model_error_pattern = re.compile(r"ERROR creating", re.IGNORECASE)
                if any(model_error_pattern.search(line) for line in build_result):
                    msg = "One or more models failed to build."
                    raise RuntimeError(msg)
        else:
            run_task = dbt_task.with_options(
                name="dbt_run", timeout_seconds=timeout_seconds
            )
            run = run_task.submit(
                project_path=dbt_project_path_full,
                command=f"run {run_select_safe} {dbt_target_option}",
            )
            run.result()

            test_task = dbt_task.with_options(
                name="dbt_test", timeout_seconds=timeout_seconds
            )
            test = test_task.submit(
                project_path=dbt_project_path_full,
                command=f"test {test_select_safe} {dbt_target_option}",
                raise_on_failure=False,
            )
            test.result()
    else:
        # Produce `catalog.json` and `manifest.json` artifacts for Luma ingestion.
        docs_generate_task = dbt_task.with_options(
            name="dbt_docs_generate", timeout_seconds=timeout_seconds
        )
        docs = docs_generate_task.submit(
            project_path=dbt_project_path_full,
            command="docs generate",
        )
        docs.result()


@flow(
    name="Transform and Catalog",
    description="Build specified dbt model(s) and upload generated metadata to Luma.",
)
@with_flow_timeout_param()
def transform_and_catalog(  # noqa: PLR0913, PLR0915 | Complexity complaints - should be gone once Luma Catalog support is deprecated.
    dbt_repo_url: str | None = None,
    dbt_repo_url_secret: str | None = None,
    dbt_project_path: str = "dbt",
    dbt_repo_branch: str | None = None,
    dbt_repo_token_secret: str | None = None,
    dbt_selects: dict[str, str] | None = None,
    dbt_target: str | None = None,
    dbt_target_dir_path: str | Path | None = None,
    luma_url: str | None = None,
    luma_follow: bool = False,
    enable_perspective: bool = False,
    perspective_api_url: str | None = None,
    perspective_api_token_secret: str | None = None,
    perspective_follow: bool = False,
    perspective_dry_run: bool = False,
    metadata_kind: Literal["model", "model_run"] = "model_run",
    run_results_storage_path: str | None = None,
    run_results_storage_config_key: str | None = None,
    run_results_storage_credentials_secret: str | None = None,
    fail_flow_only_on_build_failure: bool = False,
    model_name: str | None = None,
    manifest_path: str | None = None,
    manifest_store_type: str = "s3",
    manifest_store_credentials_secret: str | None = None,
    state_path: str | None = None,
    state_store_type: str = "s3",
    state_store_credentials_secret: str | None = None,
    trigger_downstream_models: bool = False,
    trigger_downstream_models_delay: int = 0,
    sla_breach_grace_period_minutes: int = 30,
    additional_recipients: list[str] | None = None,
    notification_recipients: list[str] | None = None,
    smtp_credential_secret: str | None = None,
    gh_action_actor: str | None = None,
    *,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> State | None:
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
            expressions. Valid keys are `run`, `test`,`build`, and `source_freshness`.
                The test select expression is taken from run's, as long as run select is
                provided. Defaults to None.
        dbt_target (str): The dbt target to use. If not specified, the default dbt
            target (as specified in `profiles.yaml`) will be used. Defaults to None.
        dbt_target_dir_path (str | Path, optional): The path to your dbt project's
            target directory, which contains dbt artifact JSON files, relative
            to dbt project's root directory. By default,
            `<repo_name>/<dbt_project_path>/target`, since "target" is the default
            name of the directory generated by dbt.
        luma_url (str, optional): The URL of the Luma instance to ingest into.
            Defaults to None. NOTE: Do not use loopback/local addresses as the default
            value or mention them in this docstring — WAF inspects the deployment
            registration request body and blocks on those strings. The actual fallback
            address is applied inside the flow body instead.
        luma_follow (bool, optional): Whether to follow the ingestion process until it's
            completed (by default, ingestion request is sent without awaiting for the
            response). By default, `False`.
        enable_perspective (bool, optional): Whether to enable Perspective ingestion.
            Defaults to False.
        perspective_api_url (str, optional): The URL of the Perspective instance to
            ingest into. Defaults to None. NOTE: Do not use loopback/local addresses as
            the default value or mention them in this docstring — WAF inspects the
            deployment registration request body and blocks on those strings. The actual
            fallback address is applied inside the flow body instead.
        perspective_api_token_secret (str | None, optional): The name of the secret
            block in Prefect holding the Perspective API token. Defaults to None.
        perspective_follow (bool, optional): Whether to follow the ingestion process
            until it's completed (by default, ingestion request is sent without awaiting
            for the response). By default, `False`.
        perspective_dry_run (bool, optional): Whether to perform a dry run of the
            ingestion process. By default, `False`.
        metadata_kind (Literal["model", "model_run"], optional): The kind of metadata
            to ingest. Defaults to "model_run".
        run_results_storage_path (str, optional): The directory to upload the
            `run_results.json` file to. Note that a timestamp will be appended to the
            end of the file. Currently, only S3 is supported. Defaults to None.
        run_results_storage_config_key (str, optional): The key in the viadot config
            holding AWS credentials. Defaults to None.
        run_results_storage_credentials_secret (str, optional): The name of the secret
            block in Prefect holding AWS credentials. Defaults to None.
        fail_flow_only_on_build_failure (bool): Whether to fail the flow **only** if the
            `dbt build` command fails.
            When False (default):
                - The flow will fail on any dbt command failure
            When True:
                - The flow will fail only on the `dbt build` command failure
            When using `dbt build`, the `run` and `test` commands are executed as part
            of the build process, and their failure is expected to be captured in the
            `run_results.json` artifact. Therefore, it's more intuitive to not fail the
            flow on `run` and `test` failures when `dbt build` is used, in order to
            allow the flow to complete and capture those failures in the metadata.
        manifest_path (str | None, optional): URI of the manifest file
            (e.g. ``"s3://bucket/manifest.json"``). Required if `state_path` is
            provided, since manifest metadata is needed to determine downstream models
            to trigger. Defaults to None.
        manifest_store_type (str, optional): Backend type for the manifest store.
            Currently only ``"s3"`` is supported. Defaults to "s3".
        manifest_store_credentials_secret (str | None, optional): Store credentials for
            the manifest. Omit to use ambient AWS credentials. Defaults to None.
        state_path (str | None, optional): URI of the state file
            (e.g. ``"s3://bucket/state.json"``). If provided, the flow will update the
            state of the executed dbt node in the state file. Defaults to None.
        state_store_type (str, optional): Backend type for the state store. Currently
            only ``"s3"`` is supported. Defaults to "s3".
        state_store_credentials_secret (str | None, optional): Store credentials. Omit
            to use ambient AWS credentials. Defaults to None.
        trigger_downstream_models (bool, optional): Whether to trigger downstream models
            by updating their state in the state file. Defaults to False.
        trigger_downstream_models_delay (int, optional): Delay in seconds before
            triggering downstream models. Defaults to 0.
        sla_breach_grace_period_minutes (int, optional): Grace period in minutes before
            an SLA breach is triggered. Defaults to 30.
        notification_recipients (list[str] | None, optional): Primary recipient list.
            If provided, it takes precedence over the extracted owners email addresses
            from dbt metadata. Defaults to None.
        additional_recipients (list[str] | None, optional): Extra email addresses
            to be appended to the final recipient list regardless of other settings.
            Defaults to None.
        smtp_credential_secret (str | None, optional): The name of the secret block in
            Prefect holding SMTP credentials. Defaults to None.
        gh_action_actor (str, optional): GitHub Actions actor that triggered the
            workflow. Defaults to None.
        timeout_seconds (int): Maximum runtime for the flow and each spawned dbt task.
            Defaults to 7200.

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

        Some common `dbt_selects` patterns:
        - runs a specific model and all its downstream dependencies:
            `dbt_select={"run": "my_model+"}`
        - runs all models in a directory:
            `dbt_select={"run: "models/staging"}`
        - runs a specific model in a folder:
            `dbt_select={"run": "marts.domain.some_model"}`
        - runs tests for a specific model:
            `dbt_select={"test": "my_model"}`
        - build a specific model:
            `dbt_select={"build": "my_model"}`
        - build all models in a folder:
            `dbt_select={"build": "models.intermediate"}`
    """
    logger = get_run_logger()
    if gh_action_actor:
        logger.info(f"Triggered by GitHub Actions actor: {gh_action_actor}")

    # Clone the dbt project.
    dbt_repo_url = dbt_repo_url or get_credentials(dbt_repo_url_secret)
    clone_repo(
        url=dbt_repo_url,
        checkout_branch=dbt_repo_branch,
        token_secret=dbt_repo_token_secret,
    )

    # Prepare the environment.
    dbt_repo_name = dbt_repo_url.split("/")[-1].replace(".git", "")
    dbt_project_path_full = Path(dbt_repo_name) / dbt_project_path
    dbt_pull_deps_task = dbt_task.with_options(
        name="dbt_deps",
        retries=3,
        retry_delay_seconds=60,
        timeout_seconds=timeout_seconds,
    )
    pull_dbt_deps = dbt_pull_deps_task.submit(
        project_path=dbt_project_path_full,
        command="deps",
    )
    pull_dbt_deps.result()

    state_update_params = {
        "node_name": model_name,
        "node_type": "model",
        "trigger_delay": trigger_downstream_models_delay,
        "sla_breach_grace_period_minutes": sla_breach_grace_period_minutes,
        "state_path": state_path,
        "state_store_type": state_store_type,
        "state_store_credentials": get_credentials(state_store_credentials_secret)
        if state_store_credentials_secret
        else None,
        "manifest_path": manifest_path,
        "manifest_store_type": manifest_store_type,
        "manifest_store_credentials": get_credentials(manifest_store_credentials_secret)
        if manifest_store_credentials_secret
        else None,
    }

    # Update node state to "running" before executing dbt commands.
    track_state = bool(state_path)
    if track_state:
        update_node_state(
            **state_update_params,
            status="running",
        )

    # Run dbt transforms and track node state.
    _node_status = "failed"
    _manifest = None
    try:
        _run_dbt_transforms(
            metadata_kind=metadata_kind,
            dbt_selects=dbt_selects,
            dbt_project_path_full=dbt_project_path_full,
            dbt_target=dbt_target,
            fail_flow_only_on_build_failure=fail_flow_only_on_build_failure,
            timeout_seconds=timeout_seconds,
        )
        _node_status = "success"
    finally:
        if track_state:
            _manifest = update_node_state(
                **state_update_params,
                status=_node_status,
            )

    if trigger_downstream_models:
        trigger_downstream_node(
            node_name=model_name,
            manifest=_manifest,
            state_path=state_path,
            state_store_credentials=state_update_params["state_store_credentials"],
        )

    # Upload metadata to Luma Catalog.
    if dbt_target_dir_path is None:
        dbt_target_dir_path = dbt_project_path_full / "target"
    else:
        dbt_target_dir_path = Path(dbt_target_dir_path)

    if luma_url:
        upload_metadata_luma = luma_ingest_task.submit(
            metadata_kind=metadata_kind,
            metadata_dir_path=dbt_target_dir_path,
            luma_url=luma_url,
            follow=luma_follow,
        )
        upload_metadata_luma.result()

    # Upload metadata to Perspective Catalog.
    # The metadata_kind check is for legacy support, since this flow used to also
    # support ingesting model metadata to Luma Catalog, but it's now suggested to do
    # this via simple CLI commands in the CI/CD pipeline.
    if enable_perspective and metadata_kind == "model_run":
        perspective_api_token = (
            get_credentials(perspective_api_token_secret)
            if perspective_api_token_secret
            else None
        )

        upload_metadata_perspective = perspective_ingest_task.submit(
            target_path=dbt_target_dir_path,
            perspective_api_url=perspective_api_url,
            perspective_api_token=perspective_api_token,
            follow=perspective_follow,
            dry_run=perspective_dry_run,
        )
        upload_metadata_perspective.result()

    file_name = "run_results.json"
    run_results_file_path = str(dbt_target_dir_path / file_name)  # type: ignore
    if run_results_storage_path:
        # Set the file path to include date info.
        now = datetime.now(timezone.utc)
        run_results_storage_path = run_results_storage_path.rstrip("/") + "/"

        # Add partitioning.
        date_str = now.strftime("%Y%m%d")
        run_results_storage_path += date_str + "/"

        # Add timestamp suffix, eg. run_results_1737556947.934292.json.
        timestamp = now.timestamp()
        run_results_storage_path += (
            Path(file_name).stem + "_" + str(timestamp) + ".json"
        )
        logger.info(f"Uploading run results to {run_results_storage_path}...")
        # Upload the file to s3.

        s3_upload_file(
            from_path=run_results_file_path,
            to_path=run_results_storage_path,
            config_key=run_results_storage_config_key,
            credentials_secret=run_results_storage_credentials_secret,
        )
    if smtp_credential_secret:
        smtp_credential = get_credentials(smtp_credential_secret)
        dbt_test_failure_notifier(
            results_file_path=run_results_file_path,
            manifest_file_path=str(dbt_target_dir_path / "manifest.json"),
            recipients=notification_recipients,
            additional_recipients=additional_recipients,
            smtp_credential=smtp_credential,
        )

    remove_dbt_repo_dir(
        dbt_repo_dir_name=dbt_repo_name,
    )

    return None
