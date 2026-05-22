"""Build specified dbt model(s) and upload the generated metadata to Luma."""

from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
from typing import Any, Literal

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.states import State

from viadot.orchestration.dbt.artifact_store import ArtifactStore
from viadot.orchestration.prefect.tasks import (
    clone_repo,
    dbt_task,
    dbt_test_failure_notifier,
    luma_ingest_task,
    perspective_ingest_task,
)
from viadot.orchestration.prefect.tasks.dbt import (
    trigger_downstream_nodes as trigger_downstream_nodes_task,
)
from viadot.orchestration.prefect.tasks.dbt import (
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
            seed_select = dbt_selects.get("seed")

            build_select_safe = f"-s {build_select}" if build_select is not None else ""
            run_select_safe = f"-s {run_select}" if run_select is not None else ""
            test_select_safe = f"-s {test_select}" if test_select is not None else ""
            seed_select_safe = f"-s {seed_select}" if seed_select is not None else ""

        if seed_select:
            seed_task = dbt_task.with_options(
                name="dbt_seed", timeout_seconds=timeout_seconds
            )
            seed = seed_task.submit(
                project_path=dbt_project_path_full,
                command=f"seed {seed_select_safe} {dbt_target_option}",
            )
            seed.result()

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


@task(name="download_dbt_partial_parse", cache_policy=None)
def _download_dbt_partial_parse(
    dbt_project_path_full: Path,
    artifact_store_path: str | None,
    artifact_store_type: str,
    artifact_store_credentials: dict[str, Any] | None,
) -> None:
    """Download dbt's partial parse cache for the cloned project.

    Args:
        dbt_project_path_full: Path to the cloned dbt project.
        artifact_store_path: Root URI for dbt artifacts.
        artifact_store_type: Backend type for the artifact store.
        artifact_store_credentials: Store credentials.
    """
    if not artifact_store_path:
        return

    logger = get_run_logger()
    dbt_target_dir_path = dbt_project_path_full / "target"
    try:
        artifact_store = ArtifactStore(artifact_store_type)
        artifact_store.download_partial_parse(
            credentials=artifact_store_credentials,
            artifact_store_path=artifact_store_path,
            target_dir_path=dbt_target_dir_path,
        )
    except Exception:
        logger.exception(
            "Could not download dbt partial parse cache from "
            f"{artifact_store_path}. dbt will parse normally."
        )


@task(name="upload_dbt_run_results", cache_policy=None)
def _upload_dbt_run_results(
    local_run_results_file_path: str,
    artifact_store_path: str | None,
    artifact_store_type: str,
    artifact_store_credentials: dict[str, Any] | None,
) -> None:
    """Upload dbt's run results artifact."""
    if not artifact_store_path:
        return

    logger = get_run_logger()
    now = datetime.now(timezone.utc)
    artifact_store = ArtifactStore(artifact_store_type)
    run_results_path = artifact_store.upload_run_results(
        credentials=artifact_store_credentials,
        artifact_store_path=artifact_store_path,
        local_run_results_file_path=local_run_results_file_path,
        date_str=now.strftime("%Y%m%d"),
        timestamp=now.timestamp(),
    )
    logger.info(f"Uploaded run results to {run_results_path}.")


@flow(
    name="Transform and Catalog",
    description="Build specified dbt model(s) and upload generated metadata to Luma.",
)
@with_flow_timeout_param()
def transform_and_catalog(  # noqa: PLR0912, PLR0913, PLR0915, C901 | Complexity complaints - should be gone once Luma Catalog support is deprecated.
    dbt_repo_url: str | None = None,
    dbt_repo_url_secret: str | None = None,
    dbt_project_path: str = "dbt",
    dbt_repo_branch: str | None = None,
    dbt_repo_token_secret: str | None = None,
    dbt_selects: dict[str, str] | None = None,
    dbt_target: str | None = None,
    dbt_target_dir_path: str | Path | None = None,
    artifact_store_path: str | None = None,
    artifact_store_type: str = "s3",
    artifact_store_credentials_secret: str | None = None,
    luma_url: str | None = None,
    luma_follow: bool = False,
    enable_perspective: bool = False,
    perspective_api_url: str | None = None,
    perspective_api_token_secret: str | None = None,
    perspective_follow: bool = False,
    perspective_dry_run: bool = False,
    metadata_kind: Literal["model", "model_run"] = "model_run",
    fail_flow_only_on_build_failure: bool = False,
    model_name: str | None = None,
    track_state: bool = False,
    state_path: str | None = None,
    state_store_type: str = "s3",
    state_store_credentials_secret: str | None = None,
    trigger_downstream_nodes: bool = False,
    trigger_downstream_nodes_delay: int = 0,
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
        artifact_store_path (str | None, optional): Root URI for dbt artifacts. When
            provided, the flow derives artifact paths under this root, e.g.
            `manifest.json`, `partial_parse.msgpack`, and timestamped run results.
            Defaults to None.
        artifact_store_type (str, optional): Backend type for the artifact store.
            Currently only ``"s3"`` is supported. Defaults to "s3".
        artifact_store_credentials_secret (str | None, optional): Store credentials for
            dbt artifacts. Omit to use ambient AWS credentials. Defaults to None.
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
        model_name (str, optional): The name of the dbt model being built. Required if
            state tracking is enabled.
        track_state (bool): Whether to track the state of the dbt node in a state file.
        state_path (str | None, optional): URI of the state file
            (e.g. ``"s3://bucket/state.json"``). If provided, the flow will update the
            state of the executed dbt node in the state file. Defaults to None.
        state_store_type (str, optional): Backend type for the state store. Currently
            only ``"s3"`` is supported. Defaults to "s3".
        state_store_credentials_secret (str | None, optional): Store credentials. Omit
            to use ambient AWS credentials. Defaults to None.
        trigger_downstream_nodes (bool, optional): Whether to trigger downstream nodes
            by updating their state in the state file. Defaults to False.
        trigger_downstream_nodes_delay (int, optional): Delay in seconds before
            triggering downstream nodes. Defaults to 0.
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
            artifact_store_path="s3://my-bucket/dbt/artifacts",
            artifact_store_credentials_secret="my-aws-credentials-block",
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
    if trigger_downstream_nodes and not track_state:
        msg = "State tracking must be enabled to trigger downstream nodes."
        raise ValueError(msg)

    if track_state and not artifact_store_path:
        msg = "Artifact store path must be provided when state tracking is enabled."
        raise ValueError(msg)

    logger = get_run_logger()

    if gh_action_actor:
        logger.info(f"Triggered by GitHub Actions actor: {gh_action_actor}")

    # Clone the dbt project.
    dbt_repo_url = dbt_repo_url or get_credentials(dbt_repo_url_secret)
    clone_repo(
        url=dbt_repo_url,
        checkout_branch=dbt_repo_branch,
        token_secret=dbt_repo_token_secret,
        depth=1,
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

    state_store_credentials = get_credentials(state_store_credentials_secret)
    artifact_store_credentials = get_credentials(artifact_store_credentials_secret)
    manifest_path = None
    if artifact_store_path:
        artifact_store = ArtifactStore(artifact_store_type)
        manifest_path = artifact_store.manifest_path(artifact_store_path)

    state_update_params = {
        "node_name": model_name,
        "node_type": "model",
        "trigger_delay": trigger_downstream_nodes_delay,
        "sla_breach_grace_period_minutes": sla_breach_grace_period_minutes,
        "state_path": state_path,
        "state_store_type": state_store_type,
        "state_store_credentials": state_store_credentials,
        "manifest_path": manifest_path,
        "artifact_store_type": artifact_store_type,
        "artifact_store_credentials": artifact_store_credentials,
    }

    _download_dbt_partial_parse(
        dbt_project_path_full=dbt_project_path_full,
        artifact_store_path=artifact_store_path,
        artifact_store_type=artifact_store_type,
        artifact_store_credentials=artifact_store_credentials,
    )

    # Update node state to "running" before executing dbt commands.
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

    if trigger_downstream_nodes:
        trigger_downstream_nodes_task(
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

    local_run_results_file_path = str(
        dbt_target_dir_path / ArtifactStore.RUN_RESULTS_FILENAME
    )
    if metadata_kind == "model_run" and artifact_store_path:
        _upload_dbt_run_results(
            local_run_results_file_path=local_run_results_file_path,
            artifact_store_path=artifact_store_path,
            artifact_store_type=artifact_store_type,
            artifact_store_credentials=artifact_store_credentials,
        )
    if smtp_credential_secret:
        smtp_credential = get_credentials(smtp_credential_secret)
        dbt_test_failure_notifier(
            results_file_path=local_run_results_file_path,
            manifest_file_path=str(dbt_target_dir_path / "manifest.json"),
            recipients=notification_recipients,
            additional_recipients=additional_recipients,
            smtp_credential=smtp_credential,
        )

    remove_dbt_repo_dir(
        dbt_repo_dir_name=dbt_repo_name,
    )

    return None
