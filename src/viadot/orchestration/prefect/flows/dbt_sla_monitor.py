"""Flow to monitor dbt model SLAs and notify owners of breaches."""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import logging
from typing import Literal

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.variables import Variable

from viadot.orchestration.dbt.state_store import StateStore
from viadot.orchestration.prefect.utils import (
    SmtpConfig,
    get_credentials,
    send_email_notification,
)


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


def _get_sla_check_inputs(
    node: dict, prefect_logger: logging.Logger
) -> tuple[str, str | None, datetime] | None:
    """Return parsed SLA inputs for eligible model nodes.

    Nodes that are not models, currently running, or missing ``fresh_until`` are
    considered ineligible and return ``None``.
    """
    node_name = node["table_name"]

    if node.get("node_type") != "model":
        prefect_logger.debug(f"Node '{node_name}' is not a model; skipping SLA check.")
        return None

    node_status = node.get("status")
    if node_status == "running":
        prefect_logger.debug(
            f"Node '{node_name}' is currently running; skipping SLA check."
        )
        return None

    fresh_until_raw = node.get("fresh_until")
    if not fresh_until_raw:
        prefect_logger.warning(
            f"No 'fresh_until' timestamp for node '{node_name}'; cannot validate SLA."
        )
        return None

    return node_name, node_status, datetime.fromisoformat(fresh_until_raw)


def _format_datetime(iso_str: str) -> str:
    """Format an ISO datetime string in a human-readable form, including timezone."""
    dt = datetime.fromisoformat(iso_str)
    tz_str = dt.strftime("%Z") or dt.strftime("%z")
    return dt.strftime("%-d %B %Y at %H:%M ") + tz_str


@task
def notify_sla_breaches(
    recipient: str,
    breaches: list[tuple[str, str]],
    smtp_credentials_secret: str,
) -> None:
    """Notify an owner of all their SLA breaches in a single email.

    Args:
        recipient: Email address of the owner to notify.
        breaches: List of ``(node_name, fresh_until)`` tuples for all breached nodes.
        smtp_credentials_secret: The name of the Prefect Secret containing SMTP
            credentials.
    """
    breach_rows = "\n".join(
        (
            "    <tr>"
            f"<td style='padding: 4px 24px 4px 0;'>{node_name}</td>"
            f"<td style='padding: 4px 0;'>{_format_datetime(fresh_until)}</td>"
            "</tr>"
        )
        for node_name, fresh_until in breaches
    )
    message = (
        f"<p>The following {len(breaches)} model(s) have breached their SLA:</p>\n"
        "<table cellpadding='0' cellspacing='0' border='0'>\n"
        "  <thead>\n"
        "    <tr>\n"
        "      <th align='left' style='padding: 0 24px 6px 0;'>Model</th>\n"
        "      <th align='left' style='padding: 0 0 6px 0;'>Fresh Until</th>\n"
        "    </tr>\n"
        "  </thead>\n"
        f"  <tbody>\n{breach_rows}\n  </tbody>\n"
        "</table>\n"
        "<p>Please investigate and take necessary action.</p>"
    )
    get_run_logger().warning(
        f"Notifying {recipient} of SLA breaches: {[b[0] for b in breaches]}."
    )
    smtp_credentials = get_credentials(smtp_credentials_secret)
    smtp_config = SmtpConfig(**smtp_credentials)
    send_email_notification(
        subject=f"SLA Breach: {len(breaches)} model(s) require attention",
        body=message,
        recipients=[recipient],
        smtp_config=smtp_config,
    )


def _notify_and_mark_breaches(
    owner_breaches: dict[str, list[tuple[str, str]]],
    dry_run: bool,
    smtp_credentials_secret: str | None,
    store: StateStore,
    prefect_logger: logging.Logger,
) -> None:
    """Notify owners and persist notification flags for breached nodes."""
    if dry_run:
        for owner, breaches in owner_breaches.items():
            prefect_logger.info(
                f"(Dry run) Would notify '{owner}' about"
                f" {len(breaches)} breach(es):"
                f" {[b[0] for b in breaches]}."
            )
        return

    if smtp_credentials_secret is None:
        prefect_logger.error(
            "SMTP credentials secret must be provided when not in dry-run mode."
        )
        return

    for owner, breaches in owner_breaches.items():
        notify_sla_breaches(
            recipient=owner,
            breaches=breaches,
            smtp_credentials_secret=smtp_credentials_secret,
        )

    # Set a flag on breached nodes to avoid sending further notifications until the
    # node is fresh again.
    unique_node_names = {
        node_name for breaches in owner_breaches.values() for node_name, _ in breaches
    }
    for node_name in unique_node_names:
        store.write(
            node_state={
                "table_name": node_name,
                "_sla_breach_notification_sent": True,
            }
        )


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

    # owner_email -> [(node_name, fresh_until), ...]
    owner_breaches: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for node in state.values():
        sla_inputs = _get_sla_check_inputs(node, prefect_logger)
        if sla_inputs is None:
            continue

        node_name, node_status, fresh_until = sla_inputs
        current_date = datetime.now(timezone.utc)

        if (
            node_status == "success"
            and node.get("_sla_breach_notification_sent")
            and current_date <= fresh_until
        ):
            # Reset SLA breach notification flag only when the node is fresh again,
            # so future breaches trigger a new notification.
            store.write(
                node_state={
                    "table_name": node_name,
                    "_sla_breach_notification_sent": False,
                }
            )
            continue

        if current_date > fresh_until + timedelta(
            minutes=node.get("sla_breach_grace_period", 30)
        ):
            if node.get("_sla_breach_notification_sent"):
                prefect_logger.debug(
                    f"Owners already notified for '{node_name}'. Skipping..."
                )
                continue

            owners = _get_node_owners(node, owner_type=owner_type)
            if not owners:
                prefect_logger.warning(
                    f"SLA breached for '{node_name}' but no '{owner_type}'"
                    " type owners defined."
                )
                continue

            prefect_logger.info(f"SLA breach detected for node '{node_name}'.")
            for owner in owners:
                owner_breaches[owner].append((node_name, node["fresh_until"]))

    if not owner_breaches:
        prefect_logger.info("No new SLA breaches to report.")
        return

    _notify_and_mark_breaches(
        owner_breaches=owner_breaches,
        dry_run=dry_run,
        smtp_credentials_secret=smtp_credentials_secret,
        store=store,
        prefect_logger=prefect_logger,
    )


if __name__ == "__main__":
    sla_monitor(
        state_path=f"s3://{Variable.get('s3_state_store_bucket')}/node-status/node-status.json",
        state_store_credentials_secret=Variable.get("state_store_credentials"),
        smtp_credentials_secret=Variable.get("smtp_credentials"),
        owner_type="technical owner",
        dry_run=True,
    )
