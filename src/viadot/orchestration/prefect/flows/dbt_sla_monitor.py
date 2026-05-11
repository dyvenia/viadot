"""Flow to monitor dbt model SLAs and notify owners of breaches."""

from datetime import datetime, timedelta, timezone
from typing import Literal

from loguru import logger
from prefect import flow, task
from prefect.variables import Variable

from viadot.orchestration.dbt.state_store import StateStore
from viadot.orchestration.prefect.utils import get_credentials, send_email_notification


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


@flow(name="SLA Monitor")
def sla_monitor(
    state_store_credentials_secret: str,
    smtp_credentials_secret: str | None = None,
    state_path: str | None = None,
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
        state_store_credentials_secret: The name of the Prefect Secret containing
            credentials to access the state store.
        smtp_credentials_secret: The name of the Prefect Secret containing SMTP
            credentials for sending notifications. Not used in dry-run mode. Defaults
            to None.
        state_path (str, optional): S3 path to the state JSON file.
            Defaults to ``s3://{s3_bucket}/deployment-status/deployment-status-concurrent.json``.
        owner_type: Owner type to notify on a breach.
            Allowed values: ``"technical owner"``, ``"business owner"``, ``"all"``.
            Defaults to ``"technical"``.
        dry_run (bool, optional): When ``True``, logs detected breaches without sending
            notifications. Defaults to ``False``.

    Notes:
        Nodes with ``state == "running"`` are skipped to avoid false SLA breach alerts
        while a pipeline is actively executing.

        The grace period for each node is read from the ``sla_breach_grace_period``
        field stored in the state file (written by the ingestion/transform flow).
        It defaults to 30 minutes when not set.
    """
    if state_path is None:
        state_path = f"s3://{Variable.get('s3_bucket')}/deployment-status/deployment-status-concurrent.json"

    store = StateStore(
        store_type="s3",
        state_path=state_path,
        credentials=get_credentials(state_store_credentials_secret),
    )
    state, _ = store._read()

    for node in state.values():
        node_name = node["table_name"]

        if node.get("TYPE") != "model":
            logger.debug(f"Node '{node_name}' is not a model; skipping SLA check.")
            continue

        if node.get("status") == "running":
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
            owners = _get_node_owners(node, owner_type=owner_type)
            if owners:
                if dry_run:
                    logger.info(
                        f"(Dry run) SLA breach detected for node '{node_name}'"
                        f" Node was fresh until: {node['fresh_until']}."
                        f" Owners to notify: {owners}."
                    )
                else:
                    notify_sla_breach(
                        node_name=node_name,
                        fresh_until=node["fresh_until"],
                        recipients=owners,
                        smtp_credentials_secret=smtp_credentials_secret,
                    )
            else:
                logger.warning(
                    f"SLA breached for '{node_name}' but no '{owner_type}' type owners defined."
                )


if __name__ == "__main__":
    state_path = f"s3://{Variable.get('s3_bucket')}/deployment-status/deployment-status-concurrent.json"
    print(state_path)
    sla_monitor(
        state_path=f"s3://{Variable.get('s3_bucket')}/deployment-status/deployment-status-concurrent.json",
        state_store_credentials_secret="redshiftspectrumsecret",
        smtp_credentials_secret="my_smtp_secret",
        owner_type="technical owner",
        dry_run=True,
    )
