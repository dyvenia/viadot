from datetime import datetime, timedelta, timezone  # noqa: D100
from typing import Any, Literal
from urllib.parse import urlparse

from prefect import get_run_logger, task
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment

from viadot.sources import S3


def safe_run_deployment(deployment_name: str, flow_name: str) -> FlowRun:
    """Run a Prefect deployment as a sub-flow ( sub process ).

    By using timeout=0, the result is not awaited. Instead, flow metadata is returned
    immediately.

    Args:
        deployment_name (str): The name of the deployment to run.
        flow_name (str): The name of the flow the deployment belongs to.

    Returns:
        FlowRun: The flow run metadata returned immediately after triggering.
    """
    logger = get_run_logger()
    fqn = f"{flow_name}/{deployment_name}"
    logger.info(f"Triggering deployment '{fqn}'...")
    return run_deployment(name=fqn, timeout=0)


def is_fresh(node: str, state: dict, reference_time: datetime | None = None) -> bool:
    """Determine if a node is fresh based on its state.

    The check applies the per-node ``trigger_delay`` stored in the state file:
        ``fresh_until > now + trigger_delay``

    Args:
        node (str): The name of the node to evaluate.
        state (dict): The node-state file content with state information.
        reference_time (datetime | None, optional): The reference time to use for
            freshness evaluation. Defaults to the current UTC time.

    Returns:
        bool: True if the node is fresh, False otherwise.
    """
    logger = get_run_logger()
    logger.info(f"Evaluating freshness for node '{node}'...")

    node_state = state.get(node)

    if not node_state or not node_state.get("fresh_until"):
        logger.info(
            f"No freshness configuration found for node '{node}'. Treating node as fresh by default..."
        )
        return True

    trigger_delay = node_state.get("trigger_delay", 0)
    reference_time = (reference_time or datetime.now(timezone.utc)).replace(
        microsecond=0
    ) + timedelta(minutes=trigger_delay)

    fresh_until_dt = datetime.fromisoformat(node_state["fresh_until"])
    if fresh_until_dt.tzinfo is None:
        fresh_until_dt = fresh_until_dt.replace(tzinfo=timezone.utc)
    is_node_fresh = fresh_until_dt > reference_time
    logger.info(
        f"Node '{node}' is {'fresh' if is_node_fresh else 'stale'} "
        f"(fresh_until: {fresh_until_dt.isoformat()}, "
        f"effective_reference: {reference_time.isoformat()}, "
        f"trigger_delay: {trigger_delay}min)."
    )

    return is_node_fresh


def get_upstreams(node_name: str, manifest: dict) -> list[str]:
    """Return all upstream node names for a given node.

    Args:
        node_name (str): The name of the node whose upstream dependencies to resolve.
        manifest (dict): The dbt manifest file content.

    Returns:
        list[str]: A list of upstream node names.
    """
    logger = get_run_logger()
    logger.info(f"Resolving upstream dependencies for node {node_name}.")
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    node = next(
        (
            n
            for n in nodes.values()
            if n["name"] == node_name and n.get("resource_type") == "model"
        ),
        None,
    )
    if not node:
        return []

    upstreams = [
        (nodes[uid] if uid in nodes else sources[uid])["name"]
        for uid in node.get("depends_on", {}).get("nodes", [])
        if uid in nodes or uid in sources
    ]
    logger.info(f"Node '{node_name}' has upstream dependencies: {upstreams}")
    return upstreams


def get_direct_downstream_nodes(node_name: str, manifest: dict) -> list[str]:
    """Return all nodes that directly depend on a given node.

    Args:
        node_name (str): The name of the node whose direct downstream nodes to resolve.
        manifest (dict): The dbt manifest file content.

    Returns:
        list[str]: A list of direct downstream node names.
    """
    logger = get_run_logger()
    logger.info(f"Resolving direct downstream nodes for node {node_name}.")
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    node_uids = {
        uid for uid, n in {**nodes, **sources}.items() if n.get("name") == node_name
    }
    downstreams = [
        n["name"]
        for n in nodes.values()
        if n.get("resource_type") == "model"
        and node_uids & set(n.get("depends_on", {}).get("nodes", []))
    ]
    logger.info(f"Node '{node_name}' has direct downstream nodes: {downstreams}")
    return downstreams


def read_node_state(
    state_path: str,
    credentials: dict[str, Any],
    table_name: str | None = None,
    store_type: Literal["s3"] = "s3",
    **store_kwargs,
) -> dict | None:
    """Read the node state from the specified store.

    Args:
        state_path (str): URI of the shared status JSON file
            (e.g. "s3://bucket/path/file.json").
        credentials (dict[str, Any]): Storage credentials.
        table_name (str | None, optional): Name of the table whose state to retrieve.
            If None, the entire state dictionary is returned. Defaults to None.
        store_type (Literal["s3"], optional): The type of state store to read from.
            Defaults to "s3".
        **store_kwargs: Additional keyword arguments passed to the underlying store
            reader.

    Returns:
        dict | None: The state payload for the given table, the entire state dictionary
            if ``table_name`` is None, or None if not found.

    Raises:
        NotImplementedError: If ``store_type`` is not supported.
    """
    if store_type == "s3":
        return _read_node_state_from_s3(
            credentials=credentials,
            state_file_path=state_path,
            table_name=table_name,
            **store_kwargs,
        )
    msg = f"State store type '{store_type}' is not supported."
    raise NotImplementedError(msg)


def _read_node_state_from_s3(
    credentials: dict[str, Any],
    state_file_path: str,
    table_name: str | None = None,
) -> dict | None:
    """Read node state from an S3 JSON file.

    Args:
        credentials (dict[str, Any]): Storage credentials.
        state_file_path (str): S3 URI of the shared status JSON file
            (e.g. "s3://bucket/path/file.json").
        table_name (str | None, optional): Name of the deployment/table whose status to
            retrieve. If None, the entire state dictionary is returned.Defaults to None.

    Returns:
        dict | None: The status of the deployment for the table or the entire state
            dictionary if ``table_name`` is None.
    """
    logger = get_run_logger()
    short_path = urlparse(state_file_path).path.lstrip("/")
    logger.info(f"Reading node table status from S3 state file {short_path}")
    s3 = S3(credentials=credentials)
    if table_name is None:
        return s3.to_dict(path=state_file_path)
    return s3.to_dict(path=state_file_path).get(table_name)


def get_runnable_nodes(
    node_name: str,
    manifest: dict,
    state_path: str,
    state_store_credentials: dict[str, Any],
) -> tuple[list[str], dict[str, list[str]]]:
    """Given a node, return its runnable direct downstream nodes.

    Args:
        node_name (str): The name of the source node.
        manifest (dict): The dbt manifest dictionary.
        state_path (str): S3 URI of the shared status JSON file.
        state_store_credentials (dict[str, Any]): Credentials for the state store.

    Returns:
        tuple[list[str], dict[str, list[str]]]: A tuple of ``(runnable, stale)`` where
            ``runnable`` is a list of node names ready to run, and ``stale`` maps each
            excluded node name to its list of stale upstream names.
    """
    logger = get_run_logger()
    logger.info("Selecting runnable downstream nodes.")
    downstream_nodes = get_direct_downstream_nodes(node_name, manifest)
    state = read_node_state(
        state_path=state_path,
        credentials=state_store_credentials,
        table_name=None,
    )
    runnable: list[str] = []
    stale: dict[str, list[str]] = {}
    for node in downstream_nodes:
        stale_upstreams = [
            u
            for u in get_upstreams(node, manifest)
            if u != node_name and not is_fresh(u, state)
        ]
        if stale_upstreams:
            stale[node] = stale_upstreams
        else:
            runnable.append(node)
    logger.info("Runnable downstream nodes selection finished.")
    return runnable, stale


@task(retries=1, retry_delay_seconds=30)
def trigger_downstream_node(
    node_name: str,
    manifest: dict,
    state_path: str,
    state_store_credentials: dict[str, Any],
    flow_name: str = "transform-and-catalog",
) -> None:
    """Trigger downstream dbt nodes that are runnable.

    Args:
        node_name (str): The name of the source table or node.
        manifest (dict): The dbt manifest dictionary.
        state_path (str): The path to the state file.
        state_store_credentials (dict[str, Any]): Credentials for the state store.
        flow_name (str): The name of the flow to run deployments for.
    """
    logger = get_run_logger()
    logger.info("Finding runnable nodes ...")
    nodes_to_run, stale_nodes = get_runnable_nodes(
        node_name=node_name,
        manifest=manifest,
        state_path=state_path,
        state_store_credentials=state_store_credentials,
    )
    if stale_nodes:
        logger.warning(
            f"The following upstream node is stale: {list(stale_nodes.keys())}."
        )
        logger.warning(
            f"Excluding downstream nodes: {[value for values in stale_nodes.values() for value in values]} from the run."
        )
    if nodes_to_run:
        logger.info("Triggering downstream nodes ...")
        for node in nodes_to_run:
            deployment_name = f"dbt_{node}"
            safe_run_deployment(
                deployment_name=deployment_name,
                flow_name=flow_name,
            )
        return
    logger.info(
        "No nodes to trigger. All downstream nodes are either up to date or no downstream nodes exist."
    )
