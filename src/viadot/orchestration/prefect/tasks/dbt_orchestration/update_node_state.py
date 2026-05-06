"""Prefect task: write node state to the state store."""

from typing import Any

from prefect import get_run_logger, task

from viadot.orchestration.dbt_dynamic.state_handler import StateHandler
from viadot.orchestration.dbt_dynamic.state_store import StateStore


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
