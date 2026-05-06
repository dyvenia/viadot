from datetime import datetime, time, timedelta, timezone  # noqa: D100
from pathlib import Path
import re
from typing import Any, Literal
from zoneinfo import ZoneInfo

from croniter import croniter
from dateutil.relativedelta import relativedelta
from prefect import get_run_logger, task
from read_write_to_s3 import create_new_state_file, update_json
from ruamel.yaml import YAML


_yaml = YAML(typ="safe", pure=True)


def get_node_meta(node_name: str, manifest: dict) -> dict:
    """Return the ``meta`` block for a dbt node (model or source) from the manifest."""
    logger = get_run_logger()
    logger.info(f"Reading node: {node_name} metadata from manifest.")
    all_nodes = {**manifest.get("nodes", {}), **manifest.get("sources", {})}
    node = next((n for n in all_nodes.values() if n.get("name") == node_name), None)
    if not node:
        return {}
    return node.get("meta") or {}


def _get_node_sla(meta: dict) -> str | None:
    """Return the SLA string from a node's meta, or None if absent."""
    logger = get_run_logger()
    logger.info("Reading SLA from manifest.")
    return meta.get("SLA")


def _get_node_owners(meta: dict) -> list[dict]:
    """Return owner dicts from a node's meta dict."""
    logger = get_run_logger()
    logger.info("Reading owners from manifest.")
    return meta.get("owners", [])


def get_source_config_prefect_yaml(
    node_name: str, deployments_dir: str | Path | None = None
) -> tuple[str, list]:
    """Retrieve the schedule config of a source node from its ingestion deployment.

    Only ingestion deployments (those with a `table` or `redshift_table` parameter)
    are considered. dbt model deployments are intentionally ignored — their freshness
    is determined entirely by whether their upstream sources are fresh.

    Returns a tuple of (table_name, crons) where crons is a list of cron schedules,
    or ("", []) if no matching deployment is found.
    """
    logger = get_run_logger()
    logger.info("Looking up source schedule configuration in deployment YAML files.")
    if not deployments_dir:
        deployments_dir = Path(__file__).parent.parent / "deployments"
    else:
        deployments_dir = Path(deployments_dir)

    base_yaml = deployments_dir / "prefect_base.yaml"
    base_content = base_yaml.read_text() if base_yaml.exists() else ""

    # Parse each deployment file individually, prepending the base file so that
    # YAML anchors defined there (e.g. &default_schedule) are available.
    for yaml_file in deployments_dir.rglob("*.yaml"):
        if yaml_file == base_yaml:
            continue
        parsed = _yaml.load(base_content + "\n" + yaml_file.read_text()) or {}
        for deployment in parsed.get("deployments", []):
            params = deployment.get("parameters") or {}
            for key in ["table", "redshift_table"]:
                if params.get(key) == node_name:
                    logger.info("Matching deployment configuration found for source.")
                    return (
                        deployment.get("name"),
                        deployment.get("schedules") or [],
                    )
    logger.info("No deployment configuration found for source.")
    return ("", [])


def parse_sla(sla_str: str) -> timedelta | relativedelta | time:
    """Parse a human-readable SLA string.

    Parses the SLA string into a ``timedelta`` (period), ``relativedelta``
    (calendar period), or a ``time`` (wall-clock deadline).

    Supported formats:
      - Hours:      ``"24 hours"``, ``"24h"``
      - Minutes:    ``"30 minutes"``, ``"30m"``
      - Days:       ``"7 days"``, ``"7d"``
      - Months:     ``"1 month"``, ``"2months"``, ``"1mo"``
      - Years:      ``"1 year"``, ``"2years"``, ``"1yr"``, ``"1y"``
      - Wall-clock: ``"10:00"``, ``"14:30"``
    """
    logger = get_run_logger()
    logger.info("Parsing SLA configuration.")
    sla_str = sla_str.strip()

    # Years — must be matched BEFORE single-char 'y' conflicts
    yr = re.fullmatch(
        r"(\d+(?:\.\d+)?)\s*(?:years?|yr?|y)",
        sla_str,
        re.IGNORECASE,
    )
    if yr:
        return relativedelta(years=int(float(yr.group(1))))

    # Months — must be matched BEFORE single-char 'm' (minutes)
    mo = re.fullmatch(
        r"(\d+(?:\.\d+)?)\s*(?:months?|mo)",
        sla_str,
        re.IGNORECASE,
    )
    if mo:
        return relativedelta(months=int(float(mo.group(1))))

    # Hours, minutes, days
    m = re.fullmatch(
        r"(\d+(?:\.\d+)?)\s*(hours?|hr?|minutes?|mins?|days?|[hmd])",
        sla_str,
        re.IGNORECASE,
    )
    if m:
        value, unit = float(m.group(1)), m.group(2)[0].lower()
        if unit == "h":
            return timedelta(hours=value)
        if unit == "m":
            return timedelta(minutes=value)
        if unit == "d":
            return timedelta(days=value)

    # Wall-clock format HH:MM
    wc = re.fullmatch(r"(\d{1,2}):(\d{2})", sla_str)
    if wc:
        return time(hour=int(wc.group(1)), minute=int(wc.group(2)))

    msg = f"Cannot parse SLA: {sla_str!r}"
    raise ValueError(msg)


def _calc_fresh_until_from_cron(
    cron: list,
    now: datetime,
) -> str | None:
    """Return the earliest next cron run time as an ISO string, or None."""
    next_times = []
    for c in cron:
        if isinstance(c, dict):
            tz = ZoneInfo(c.get("timezone", "UTC"))
            c_iter = croniter(c["cron"], now.astimezone(tz))
            next_times.append(c_iter.get_next(datetime).astimezone(timezone.utc))
        else:
            c_iter = croniter(c, now)
            next_times.append(c_iter.get_next(datetime).astimezone(timezone.utc))

    if not next_times:
        return None
    return min(next_times).isoformat()


def _calc_fresh_until_from_sla(
    sla: str,
    now: datetime,
) -> str | None:
    """Return the SLA-based fresh_until as an ISO string, or None if SLA is ignored."""
    if sla.lower() in ("ignored", "n/a"):
        return None
    parsed_sla = parse_sla(sla)
    if parsed_sla:
        if isinstance(parsed_sla, timedelta | relativedelta):
            fresh_until_dt = now + parsed_sla
            return fresh_until_dt.isoformat()
        # Wall-clock SLA: use next occurrence of the wall-clock time
        next_dt = now.replace(
            hour=parsed_sla.hour, minute=parsed_sla.minute, second=0, microsecond=0
        )
        if next_dt <= now:
            next_dt += timedelta(days=1)
        return next_dt.isoformat()
    return None


def _calc_fresh_until(
    cron: list | None,
    sla: str | None,
    reference_time: datetime | None = None,
) -> str | None:
    """Calculate the fresh_until timestamp based on CRON schedules or SLA.

    This function determines the next freshness deadline for a node with priority
    given to CRON schedules and then SLA.
    The logic is as follows:
        1.CRON schedules: It computes the earliest next cron run time.
        2.SLA configuration: It applies SLA rules (timedelta or wall-clock).

    Args:
        sla (str | None): SLA string for nodes or will be skipped when None).
        cron (list | None): List of cron schedules for sources. Each item can be a
            dict with "cron" and "timezone" keys, or a cron string.
        reference_time (datetime | None): The UTC datetime to base calculations on.
            Defaults to ``datetime.now(timezone.utc)``.

    Returns:
        str | None: The ISO-formatted fresh_until timestamp as a string, or None if
            calculation is not possible (e.g., no cron , no SLA ).
    """
    logger = get_run_logger()
    logger.info("Calculating fresh until ...")
    now = reference_time or datetime.now(timezone.utc)
    if cron:
        return _calc_fresh_until_from_cron(cron, now)
    if sla is not None:
        return _calc_fresh_until_from_sla(sla, now)
    logger.warning("Cannot calculate fresh_until. Setting fresh_until to None.")
    return None


@task(retries=3, retry_delay_seconds=10)
def update_node_state(  # noqa: PLR0913
    table_name: str,
    status: str,
    state_path: str,
    node_type: str,
    credentials: dict[str, Any],
    sla: str | None = None,
    owners: list[dict] | None = None,
    effective_source_data_slot: str | None = None,
    batch_id: int | None = None,
    cron: list | None = None,
    trigger_delay: int = 0,
    sla_breach_grace_period: int = 30,
    store_type: Literal["s3"] = "s3",
    **store_kwargs,
) -> None:
    """Write node state to the specified store."""
    logger = get_run_logger()
    logger.info("Preparing deployment status update ...")
    now = datetime.now(timezone.utc)
    fresh_until = None
    # If no success, fresh_until is preserved from existing state
    if status == "success":
        fresh_until = _calc_fresh_until(cron, sla, now)
    state = {
        "table_name": table_name,
        "node_type": node_type,
        "status": status,
        "last_refreshed_at": now.isoformat(),
        "fresh_until": fresh_until,
        "SLA": sla,
        "owners": owners,
        "effective_source_data_slot": effective_source_data_slot,
        "batch_id": batch_id,
        "cron": cron,
        "trigger_delay": trigger_delay,
        "sla_breach_grace_period": sla_breach_grace_period,
    }
    if store_type == "s3":
        return _write_node_state_to_s3(
            state=state,
            state_file_path=state_path,
            credentials=credentials,
            **store_kwargs,
        )
    msg = f"State store type '{store_type}' is not supported."
    raise NotImplementedError(msg)


def _write_node_state_to_s3(
    state: dict,
    credentials: dict[str, Any],
    state_file_path: str,
) -> None:
    """Store model state to an S3 JSON file.

    The file is located at the provided `state_file_path` (S3 URI). The file is
    expected to contain a mapping of table_name -> payload. This function will
    update the entry for ``state["table_name"]``.

    Args:
        state: Full node state dict with all fields.
        credentials: Storage credentials.
        state_file_path: S3 URI of the shared status JSON file (e.g. ``s3://bucket/path.json``).
    """
    logger = get_run_logger()
    table_name = state["table_name"]
    try:
        update_json(
            state_file_path=state_file_path,
            state=state,
            max_retries=3,
            aws_credentials=credentials,
        )
        logger.info("Deployment status updated in S3.")
    except FileNotFoundError:
        create_new_state_file(state_file_path, table_name, state, credentials)
