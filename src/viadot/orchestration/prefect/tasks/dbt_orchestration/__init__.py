"""Prefect tasks for dbt orchestration."""

from typing import Any, Literal

from prefect import get_run_logger, task
from prefect.deployments import run_deployment

from viadot.orchestration.dbt_dynamic.manifest_handler import ManifestHandler
from viadot.orchestration.dbt_dynamic.manifest_store import ManifestStore
from viadot.orchestration.dbt_dynamic.state_handler import StateHandler
from viadot.orchestration.dbt_dynamic.state_store import StateStore


@task(retries=1, retry_delay_seconds=10)
def read_dbt_manifest(
    credentials: dict[str, Any],
    store_type: Literal["s3"] = "s3",
    **store_kwargs,
) -> dict:
    """Read the dbt manifest from the specified store and return it as a dict.

    Args:
        credentials: Store credentials.
        store_type: Type of the storage (e.g. "s3").
    """
    logger = get_run_logger()
    logger.info("Reading dbt manifest from configured store.")
    store = ManifestStore(store_type=store_type)
    return store.read(
        credentials=credentials,
        **store_kwargs,
    )


@task(retries=3, retry_delay_seconds=10)
def update_node_state(  # noqa: PLR0913
    table_name: str,
    status: str,
    node_type: str,
    state_path: str,
    credentials: dict[str, Any] | None = None,
    store_type: str = "s3",
    sla: str | None = None,
    owners: list[dict] | None = None,
    effective_source_data_slot: str | None = None,
    batch_id: int | None = None,
    cron: list | None = None,
    trigger_delay: int = 0,
    sla_breach_grace_period: int = 30,
) -> None:
    """Build and write node state to the state store.

    Args:
        table_name: The dbt node name (model or source).
        status: Current run status (e.g. ``"success"``, ``"failed"``).
        node_type: The dbt node type (e.g. ``"model"``, ``"source"``).
        state_path: URI of the state file (e.g. ``"s3://bucket/state.json"``).
        credentials: Store credentials. Omit to use ambient AWS credentials.
        store_type: Backend type. Currently only ``"s3"`` is supported.
        sla: Optional SLA string (e.g. ``"24h"``, ``"10:00"``).
        owners: Optional list of owner dicts.
        effective_source_data_slot: Optional effective source data slot.
        batch_id: Optional batch identifier.
        cron: Optional list of cron schedule dicts or strings.
        trigger_delay: Delay in minutes before triggering downstream nodes.
        sla_breach_grace_period: Grace period in minutes before an SLA breach.
    """
    logger = get_run_logger()
    logger.info("Preparing deployment status update ...")
    store = StateStore(store_type, state_path, credentials)
    handler = StateHandler(store)
    node_state = handler.build_node_state(
        table_name=table_name,
        status=status,
        node_type=node_type,
        sla=sla,
        owners=owners,
        effective_source_data_slot=effective_source_data_slot,
        batch_id=batch_id,
        cron=cron,
        trigger_delay=trigger_delay,
        sla_breach_grace_period=sla_breach_grace_period,
    )
    handler.update(node_state)
    logger.info("Deployment status updated successfully.")


@task(retries=1, retry_delay_seconds=30)
def trigger_downstream_node(
    node_name: str,
    manifest: dict,
    state_path: str,
    state_store_credentials: dict[str, Any],
    flow_name: str = "transform-and-catalog",
) -> None:
    """Trigger downstream dbt nodes whose upstream dependencies are all fresh.

    Args:
        node_name: The name of the source table or node that just completed.
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
