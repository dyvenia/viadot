"""Flow to monitor dbt model SLAs and notify owners of breaches."""

from datetime import datetime, timedelta, timezone
import logging
from typing import Literal

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.variables import Variable

from viadot.orchestration.dbt.state_store import StateStore
from viadot.orchestration.prefect.utils import get_credentials, send_email_notification


logger = logging.getLogger(__name__)


def _get_node_owners(
    node: dict, owner_type: Literal["technical owner", "business owner", "all"]
) -> list[str]:
    """Return email addresses from a node's owners list, filtered by type.

    Args:
        node: Node state dict as stored by ``StateHandler.build_node_state``.
        owner_type: Which owner type(s) to include.

    Returns:
        List of email addresses.
    """
    owners: list[dict] = node.get("owners") or []
    if owner_type == "all":
        return [o["email"] for o in owners if o.get("email")]
    return [
        o["email"]
        for o in owners
        if o.get("email") and o.get("type", "").lower() == owner_type.lower()
    ]


@task
def notify_sla_breach(
    node_name: str,
    fresh_until: str,
    recipients: list[str],
    smtp_credentials_secret: str,
) -> None:
    """Notify model owners that the node's SLA has been breached."""
    message = (
        f"SLA breached for model '{node_name}'."
        f" Node was fresh until: {fresh_until}."
        " Please investigate and take necessary action."
    )
    logger.warning(message)
    smtp_credentials = get_credentials(smtp_credentials_secret)
    for recipient in recipients:
        send_email_notification(
            subject=f"SLA Breach: {node_name}",
            body=message,
            recipients=[recipient],
            smtp_config=smtp_credentials,
        )


def _handle_breached_node(
    store: StateStore,
    node: dict,
    node_name: str,
    owner_type: Literal["technical owner", "business owner", "all"],
    smtp_credentials_secret: str | None,
    dry_run: bool,
) -> None:
    """Notify node owners once for a breach and persist the notification flag."""
    owners = _get_node_owners(node, owner_type=owner_type)
    if not owners:
        logger.warning(
            f"SLA breached for '{node_name}' but no '{owner_type}' type owners defined."
        )
        return

    if dry_run:
        logger.info(
            f"(Dry run) SLA breach detected for node '{node_name}'"
            f" Node was fresh until: {node['fresh_until']}."
            f" Owners to notify: {owners}."
        )

    if node.get("_sla_breach_notification_sent"):
        logger.info("Owners already notified. Skipping...")
        return

    notify_sla_breach(
        node_name=node_name,
        fresh_until=node["fresh_until"],
        recipients=owners,
        smtp_credentials_secret=smtp_credentials_secret,
    )
    store.write(node_state={node_name: {"_sla_breach_notification_sent": True}})


@flow(name="SLA Monitor")
def sla_monitor(
    state_path: str,
    state_store_credentials_secret: str,
    state_store_type: str = "s3",
    smtp_credentials_secret: str | None = None,
    owner_type: Literal["technical owner", "business owner", "all"] = "technical owner",
    dry_run: bool = False,
) -> None:
    """Check SLA compliance for all dbt models and notify owners of any breaches.

    Intended to run on a frequent schedule (e.g. every 15 minutes). Reads the state
    table once from S3 and iterates the manifest nodes to find models with an SLA
    defined. File I/O is only performed when a breach is detected (to look up owners).

    Note: SLA values are read directly from the manifest, which dbt populates from the
    ``meta`` block in properties YAML files during compilation.

    Args:
        state_path (str): Path to the state store.
        state_store_credentials_secret: The name of the Prefect Secret containing
            credentials to access the state store.
        state_store_type: Backend type for the state store. Defaults to "s3".
        smtp_credentials_secret: The name of the Prefect Secret containing SMTP
            credentials for sending notifications. Not used in dry-run mode. Defaults
            to None.
        owner_type: Owner type to notify on a breach.
            Allowed values: ``"technical owner"``, ``"business owner"``, ``"all"``.
            Defaults to ``"technical owner"``.
        dry_run (bool, optional): When ``True``, logs detected breaches without sending
            notifications. Defaults to ``False``.

    Notes:
        Nodes with ``state == "running"`` are skipped to avoid false SLA breach alerts
        while a pipeline is actively executing.

        The grace period for each node is read from the ``sla_breach_grace_period``
        field stored in the state file (written by the ingestion/transform flow).
        It defaults to 30 minutes when not set.
    """
    prefect_logger = get_run_logger()
    prefect_logger.info("Starting SLA monitor flow...")

    store = StateStore(
        store_type=state_store_type,
        state_path=state_path,
        credentials=get_credentials(state_store_credentials_secret),
    )
    state, _ = store._read()

    prefect_logger.info(f"Checking SLA compliance for {len(state)} nodes...")

    for node in state.values():
        node_name = node["table_name"]
        node_status = node.get("status")

        if node_status == "success" and node.get("_sla_breach_notification_sent"):
            # Reset SLA breach notification flag on successful runs to allow future
            # breach notifications.
            prefect_logger.info("Entered node status success")
            store.write(
                node_state={node_name: {"_sla_breach_notification_sent": False}}
            )
            continue

        if node.get("TYPE") != "model":
            logger.debug(f"Node '{node_name}' is not a model; skipping SLA check.")
            continue

        if node_status == "running":
            logger.debug(
                f"Node '{node_name}' is currently running; skipping SLA check."
            )
            continue

        if node.get("fresh_until") is None:
            logger.warning(f"No SLA found for node '{node_name}'; cannot validate.")
            continue

        if datetime.now(timezone.utc) > datetime.fromisoformat(
            node["fresh_until"]
        ) + timedelta(minutes=node.get("sla_breach_grace_period", 30)):
            prefect_logger.info(f"SLA breach detected for node '{node_name}'.")
            _handle_breached_node(
                store=store,
                node=node,
                node_name=node_name,
                owner_type=owner_type,
                smtp_credentials_secret=smtp_credentials_secret,
                dry_run=dry_run,
            )


if __name__ == "__main__":
    sla_monitor(
        state_path=f"s3://{Variable.get('s3_state_store_bucket')}/node-status/node-status.json",
        state_store_credentials_secret=Variable.get("state_store_credentials"),
        smtp_credentials_secret=Variable.get("smtp_credentials"),
        owner_type="technical owner",
        dry_run=True,
    )
