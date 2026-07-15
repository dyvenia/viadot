"""Prefect tasks for dbt orchestration."""

import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Literal

from prefect import get_run_logger, task
from prefect.deployments import run_deployment
from prefect.exceptions import ObjectNotFound
from prefect.runtime import flow_run

from viadot.orchestration.dbt.artifact_store import ArtifactStore
from viadot.orchestration.dbt.manifest_handler import ManifestHandler
from viadot.orchestration.dbt.state_handler import StateHandler
from viadot.orchestration.dbt.state_store import StateStore
from viadot.orchestration.prefect.tasks.dbt.utils import get_node_schedules_prefect_yaml
from viadot.orchestration.prefect.utils import shell_run_command


@task(retries=0, timeout_seconds=2 * 60 * 60)
async def dbt_task(
    command: str = "run",
    project_path: str | Path | None = None,
    env: dict[str, Any] | None = None,
    shell: str = "bash",
    return_all: bool = False,
    stream_level: int = logging.INFO,
    raise_on_failure: bool = True,
) -> list[str] | str:
    """Runs dbt commands within a shell.

    Args:
        command: dbt command to be executed; can also be provided post-initialization
            by calling this task instance.
        project_path: The path to the dbt project.
        env: Dictionary of environment variables to use for the subprocess; can also be
            provided at runtime.
        shell: Shell to run the command with.
        return_all: Whether this task should return all lines of stdout as a list, or
            just the last line as a string.
        stream_level: The logging level of the stream; defaults to 20, equivalent to
            `logging.INFO`.
        raise_on_failure: Whether to fail the task if the command fails.

    Returns:
        If return all, returns all lines as a list; else the last line as a string.

    Example:
        Executes `dbt run` on a specified dbt project.
        ```python
        from prefect import flow
        from viadot.tasks import dbt_task

        PROJECT_PATH = "/home/viadot/dbt/my_dbt_project"

        @flow
        def example_dbt_task_flow():
            return dbt_task(
                command="run", project_path=PROJECT_PATH, return_all=True
            )

        example_dbt_task_flow()
        ```
    """
    logger = get_run_logger()

    project_path = os.path.expandvars(project_path) if project_path is not None else "."

    return await shell_run_command(
        command=f"dbt {command}",
        env=env,
        helper_command=f"cd {project_path}",
        shell=shell,
        return_all=return_all,
        stream_level=stream_level,
        raise_on_failure=raise_on_failure,
        logger=logger,
    )


def _require_event_id(event_id: str | None) -> str:
    """Return a validated event identifier."""
    if not isinstance(event_id, str) or not event_id.strip():
        msg = "event_id must be a non-empty string in event mode."
        raise ValueError(msg)
    return event_id


def _select_runnable_nodes(
    handler: ManifestHandler,
    node_name: str,
    state: dict,
    mode: Literal["freshness", "event"],
    event_id: str | None,
) -> tuple[list[str], dict[str, list[str]]]:
    """Select runnable nodes using the requested readiness policy."""
    if mode == "event":
        return handler.get_runnable_nodes_for_event(
            node_name, state, _require_event_id(event_id)
        )
    if mode == "freshness":
        if event_id is not None:
            msg = "event_id can only be provided when mode='event'."
            raise ValueError(msg)
        return handler.get_runnable_nodes(node_name, state)
    msg = f"Unsupported downstream trigger mode: {mode!r}."
    raise ValueError(msg)


def _run_downstream_deployment(
    deployment_name: str,
    tags: list[str] | None,
    mode: Literal["freshness", "event"],
    event_id: str | None,
) -> None:
    """Dispatch a downstream deployment with event idempotency when requested."""
    if mode == "freshness":
        run_deployment(name=deployment_name, timeout=0, tags=tags)
        return

    validated_event_id = _require_event_id(event_id)
    idempotency_payload = f"{deployment_name}\0{validated_event_id}".encode()
    idempotency_key = (
        "viadot-dbt-event-v1:" + hashlib.sha256(idempotency_payload).hexdigest()
    )
    run_deployment(
        name=deployment_name,
        parameters={
            "trigger_downstream_nodes_mode": "event",
            "trigger_downstream_nodes_event_id": validated_event_id,
        },
        timeout=0,
        tags=tags,
        idempotency_key=idempotency_key,
        as_subflow=False,
    )


@task(retries=3, retry_delay_seconds=10, timeout_seconds=10 * 60)
def update_node_state(  # noqa: PLR0913
    node_name: str,
    status: str,
    node_type: str,
    state_path: str,
    state_store_type: str,
    artifact_store_path: str,
    artifact_store_type: str,
    state_store_credentials: dict[str, Any] | None = None,
    artifact_store_credentials: dict[str, Any] | None = None,
    deployments_dir: str | Path | None = None,
    trigger_delay: int = 0,
    sla_breach_grace_period_minutes: int = 30,
    sla_default_timezone: str = "UTC",
    event_id: str | None = None,
) -> dict:
    """Build and write node state to the state store.

    Args:
        node_name: The dbt node name (model or source).
        status: Current run status (e.g. ``"success"``, ``"failed"``).
        node_type: The dbt node type (e.g. ``"model"``, ``"source"``).
        state_path: URI of the state file (e.g. ``"s3://bucket/state.json"``).
        state_store_type: Backend type for the state store.
        artifact_store_path: URI of the artifact store
        (e.g. ``"s3://bucket/artifacts"``).
        artifact_store_type: Backend type for the artifact store.
        state_store_credentials: Store credentials for the state store. Omit to use
            ambient AWS credentials.
        artifact_store_credentials: Store credentials for the artifact store. Omit to
            use ambient AWS credentials.
        deployments_dir: Directory containing Prefect deployment YAML files, used to
            retrieve the schedules in case the node is a source node. If not provided,
            defaults to ``<this_file's_parent>/../../deployments``.
        trigger_delay: Delay in minutes before triggering downstream nodes.
        sla_breach_grace_period_minutes: Grace period in minutes before an SLA breach.
        sla_default_timezone: Optional timezone used as default for model cron-based SLA
            dict entries that omit a timezone.
        event_id: Optional logical attempt identifier used by event-driven triggering.

    Returns:
        The dbt manifest dict (re-used by callers to avoid a second store read).
    """
    logger = get_run_logger()
    logger.info(f"Updating node status in {state_path}...")
    state_store = StateStore(state_store_type, state_path, state_store_credentials)
    logger.info("State store loaded successfully.")
    state_handler = StateHandler(state_store)
    artifact_store = ArtifactStore(artifact_store_type)
    manifest = artifact_store.read_manifest(
        credentials=artifact_store_credentials, artifact_store_path=artifact_store_path
    )
    logger.info("Artifact store loaded successfully.")
    manifest_handler = ManifestHandler(manifest)
    meta = manifest_handler.get_node_meta(node_name)

    schedules = None
    if node_type == "source":
        schedules = get_node_schedules_prefect_yaml(
            node_name, deployments_dir=deployments_dir
        )

    build_state_params = {
        "node_name": node_name,
        "status": status,
        "node_type": node_type,
        "sla": meta.get("SLA"),
        "sla_default_timezone": sla_default_timezone,
        "sla_breach_grace_period_minutes": sla_breach_grace_period_minutes,
        "owners": meta.get("owners"),
        "schedules": schedules,
        "trigger_delay": trigger_delay,
    }
    if event_id is not None:
        runtime_flow_run_id = flow_run.id
        build_state_params.update(
            event_id=event_id,
            flow_run_id=str(runtime_flow_run_id) if runtime_flow_run_id else None,
        )
    node_state = state_handler.build_node_state(
        **build_state_params,
    )
    state_handler.update(node_state)
    logger.info("Deployment status updated successfully.")
    return manifest


@task(retries=1, retry_delay_seconds=30, timeout_seconds=10 * 60)
def trigger_downstream_nodes(
    node_name: str,
    manifest: dict,
    state_path: str,
    state_store_credentials: dict[str, Any] | None,
    flow_name: str = "Transform and Catalog",
    tags: list[str] | None = None,
    on_missing_downstream_deployment: Literal["warn", "raise"] = "raise",
    mode: Literal["freshness", "event"] = "freshness",
    event_id: str | None = None,
) -> None:
    """Trigger downstream dbt nodes whose upstream dependencies are all fresh.

    Args:
        node_name: The name of the source table or model whose downstream dependencies
            should be checked and triggered.
        manifest: The dbt manifest dictionary.
        state_path: URI of the state file (e.g. ``"s3://bucket/state.json"``).
        state_store_credentials: AWS credentials for the state store.
        flow_name: The name of the Prefect flow that owns the downstream deployments.
        tags: Optional list of tags to apply to triggered deployments.
        on_missing_downstream_deployment: Behavior when a downstream Prefect
            deployment is missing. ``"warn"`` logs and continues, while
            ``"raise"`` fails the task.
        mode: ``"freshness"`` preserves legacy SLA-based gating. ``"event"``
            requires exact logical-attempt success from all cycle-bound parents.
        event_id: Opaque logical attempt identifier. Required in event mode and
            propagated to downstream deployments.
    """
    logger = get_run_logger()
    logger.info("Finding runnable nodes ...")

    store = StateStore(
        store_type="s3", state_path=state_path, credentials=state_store_credentials
    )
    state, _ = store._read()

    handler = ManifestHandler(manifest)
    nodes_to_run, stale_nodes = _select_runnable_nodes(
        handler, node_name, state, mode, event_id
    )

    if stale_nodes:
        logger.warning(
            f"The following nodes have stale upstreams: {list(stale_nodes.keys())}."
        )
        logger.warning(
            "Excluding downstream nodes: "
            f"{[v for values in stale_nodes.values() for v in values]} from the run."
        )
    if nodes_to_run:
        logger.info("Triggering downstream nodes ...")
        for node in nodes_to_run:
            # Use ``timeout=0`` so flow metadata is returned immediately.
            deployment_name = f"{flow_name}/dbt_{node}"
            try:
                _run_downstream_deployment(deployment_name, tags, mode, event_id)
            except ObjectNotFound:
                if on_missing_downstream_deployment == "warn":
                    logger.warning(
                        "Skipping missing downstream deployment '%s'.",
                        deployment_name,
                    )
                    continue
                raise
        return
    logger.info(
        "No nodes to trigger. All downstream nodes are either up to date or "
        "no downstream nodes exist."
    )
