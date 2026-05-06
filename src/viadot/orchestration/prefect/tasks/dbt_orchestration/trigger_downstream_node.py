"""Prefect task: trigger downstream dbt nodes."""

from typing import Any

from prefect import get_run_logger, task
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment

from viadot.orchestration.dbt_dynamic.manifest_handler import ManifestHandler
from viadot.orchestration.dbt_dynamic.state_store import StateStore


def safe_run_deployment(deployment_name: str, flow_name: str) -> FlowRun:
    """Run a Prefect deployment as a sub-flow without awaiting the result.

    Uses ``timeout=0`` so flow metadata is returned immediately.

    Args:
        deployment_name: The name of the deployment to run.
        flow_name: The name of the flow the deployment belongs to.

    Returns:
        FlowRun metadata returned immediately after triggering.
    """
    logger = get_run_logger()
    fqn = f"{flow_name}/{deployment_name}"
    logger.info(f"Triggering deployment '{fqn}'...")
    return run_deployment(name=fqn, timeout=0)


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
        store_type="s3",
        state_path=state_path,
        credentials=state_store_credentials,
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
            safe_run_deployment(
                deployment_name=f"dbt_{node}",
                flow_name=flow_name,
            )
        return
    logger.info(
        "No nodes to trigger. All downstream nodes are either up to date or "
        "no downstream nodes exist."
    )
