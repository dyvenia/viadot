"""Prefect tasks for dbt orchestration."""

import logging
import os
from pathlib import Path
from typing import Any

from prefect import get_run_logger, task
from prefect.deployments import run_deployment

from viadot.orchestration.dbt.manifest_handler import ManifestHandler
from viadot.orchestration.dbt.manifest_store import ManifestStore
from viadot.orchestration.dbt.state_handler import StateHandler
from viadot.orchestration.dbt.state_store import StateStore
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


@task(retries=3, retry_delay_seconds=10, timeout_seconds=10 * 60)
def update_node_state(  # noqa: PLR0913
    node_name: str,
    status: str,
    node_type: str,
    state_path: str,
    state_store_type: str,
    manifest_path: str,
    manifest_store_type: str,
    state_store_credentials: dict[str, Any] | None = None,
    manifest_store_credentials: dict[str, Any] | None = None,
    effective_source_data_slot: str | None = None,
    batch_id: int | None = None,
    crons: list | None = None,
    trigger_delay: int = 0,
    sla_breach_grace_period_minutes: int = 30,
) -> dict:
    """Build and write node state to the state store.

    Args:
        node_name: The dbt node name (model or source).
        status: Current run status (e.g. ``"success"``, ``"failed"``).
        node_type: The dbt node type (e.g. ``"model"``, ``"source"``).
        state_path: URI of the state file (e.g. ``"s3://bucket/state.json"``).
        state_store_type: Backend type for the state store.
        manifest_path: URI of the manifest file (e.g. ``"s3://bucket/manifest.json"``).
        manifest_store_type: Backend type for the manifest store.
        state_store_credentials: Store credentials for the state store. Omit to use
            ambient AWS credentials.
        manifest_store_credentials: Store credentials for the manifest store. Omit to
            use ambient AWS credentials.
        effective_source_data_slot: Optional effective source data slot.
        batch_id: Optional batch identifier.
        crons: Optional list of cron schedule dicts or strings.
        trigger_delay: Delay in minutes before triggering downstream nodes.
        sla_breach_grace_period_minutes: Grace period in minutes before an SLA breach.

    Returns:
        The dbt manifest dict (re-used by callers to avoid a second store read).
    """
    logger = get_run_logger()
    logger.info(f"Updating node status in {state_path}...")
    state_store = StateStore(state_store_type, state_path, state_store_credentials)
    state_handler = StateHandler(state_store)
    manifest_store = ManifestStore(manifest_store_type)
    manifest = manifest_store.read(
        credentials=manifest_store_credentials, path=manifest_path
    )
    manifest_handler = ManifestHandler(manifest)
    meta = manifest_handler.get_node_meta(node_name)
    node_state = state_handler.build_node_state(
        node_name=node_name,
        status=status,
        node_type=node_type,
        sla=meta.get("SLA"),
        owners=meta.get("owners"),
        effective_source_data_slot=effective_source_data_slot,
        batch_id=batch_id,
        crons=crons,
        trigger_delay=trigger_delay,
        sla_breach_grace_period_minutes=sla_breach_grace_period_minutes,
    )
    state_handler.update(node_state)
    logger.info("Deployment status updated successfully.")
    return manifest


@task(retries=1, retry_delay_seconds=30, timeout_seconds=10 * 60)
def trigger_downstream_nodes(
    node_name: str,
    manifest: dict,
    state_path: str,
    state_store_credentials: dict[str, Any],
    flow_name: str = "transform-and-catalog",
) -> None:
    """Trigger downstream dbt nodes whose upstream dependencies are all fresh.

    Args:
        node_name: The name of the source table or model whose downstream dependencies
            should be checked and triggered.
        manifest: The dbt manifest dictionary.
        state_path: URI of the state file (e.g. ``"s3://bucket/state.json"``).
        state_store_credentials: AWS credentials for the state store.
        flow_name: The name of the Prefect flow that owns the downstream deployments.
    """
    logger = get_run_logger()
    logger.info("Finding runnable nodes ...")

    store = StateStore(
        store_type="s3", state_path=state_path, credentials=state_store_credentials
    )
    state, _ = store._read()

    handler = ManifestHandler(manifest)
    nodes_to_run, stale_nodes = handler.get_runnable_nodes(node_name, state)

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
            run_deployment(name=f"{flow_name}/dbt_{node}", timeout=0)
        return
    logger.info(
        "No nodes to trigger. All downstream nodes are either up to date or "
        "no downstream nodes exist."
    )
