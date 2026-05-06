"""State management logic for dbt dynamic orchestration."""

from datetime import datetime, time, timedelta, timezone
import logging
import re
from zoneinfo import ZoneInfo

from croniter import croniter
from dateutil.relativedelta import relativedelta

from viadot.orchestration.dbt_dynamic.state_store import StateStore


logger = logging.getLogger(__name__)


# Freshness check (module-level so ManifestHandler can import it without
# creating a circular dependency between the two handler modules).
def is_fresh(node: str, state: dict, reference_time: datetime | None = None) -> bool:
    """Determine whether a node is fresh based on the state dict.

    The check applies the per-node ``trigger_delay`` stored in the state file:
    ``fresh_until > now + trigger_delay``.

    Args:
        node: Short name of the node to evaluate.
        state: Full state dict (mapping of node name → node state payload).
        reference_time: Reference UTC datetime. Defaults to
            ``datetime.now(timezone.utc)``.

    Returns:
        ``True`` if the node is fresh (or has no freshness configuration).
    """
    logger.info(f"Evaluating freshness for node '{node}'...")
    node_state = state.get(node)
    if not node_state or not node_state.get("fresh_until"):
        logger.info(
            f"No freshness configuration found for node '{node}'. "
            "Treating as fresh by default."
        )
        return True

    trigger_delay = node_state.get("trigger_delay", 0)
    effective_time = (reference_time or datetime.now(timezone.utc)).replace(
        microsecond=0
    ) + timedelta(minutes=trigger_delay)

    fresh_until_dt = datetime.fromisoformat(node_state["fresh_until"])
    if fresh_until_dt.tzinfo is None:
        fresh_until_dt = fresh_until_dt.replace(tzinfo=timezone.utc)

    is_node_fresh = fresh_until_dt > effective_time
    logger.info(
        f"Node '{node}' is {'fresh' if is_node_fresh else 'stale'} "
        f"(fresh_until: {fresh_until_dt.isoformat()}, "
        f"effective_reference: {effective_time.isoformat()}, "
        f"trigger_delay: {trigger_delay}min)."
    )
    return is_node_fresh


class StateHandler:
    """Orchestrates state reads and writes for a single dbt node.

    This class is a pure-Python layer with no Prefect dependency. It composes
    a ``StateStore`` (I/O) with the SLA / freshness logic to build and persist
    node state payloads.

    Args:
        store: An initialised ``StateStore`` instance.
    """

    def __init__(self, store: StateStore) -> None:
        self.store = store

    @staticmethod
    def _parse_sla(sla_str: str) -> timedelta | relativedelta | time:
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
        logger.info("Parsing SLA configuration.")
        sla_str = sla_str.strip()

        # Years — must be matched BEFORE single-char 'y' conflicts
        yr = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(?:years?|yr?|y)", sla_str, re.IGNORECASE)
        if yr:
            return relativedelta(years=int(float(yr.group(1))))

        # Months — must be matched BEFORE single-char 'm' (minutes)
        mo = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(?:months?|mo)", sla_str, re.IGNORECASE)
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

    @staticmethod
    def _calc_fresh_until_from_cron(cron: list, now: datetime) -> str | None:
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

    @staticmethod
    def _calc_fresh_until_from_sla(sla: str, now: datetime) -> str | None:
        """Return the SLA-based fresh_until, or None if SLA is ignored."""
        if sla.lower() in ("ignored", "n/a"):
            return None
        parsed_sla = StateHandler._parse_sla(sla)
        if parsed_sla:
            if isinstance(parsed_sla, timedelta | relativedelta):
                return (now + parsed_sla).isoformat()
            # Wall-clock SLA: use next occurrence of the wall-clock time
            next_dt = now.replace(
                hour=parsed_sla.hour, minute=parsed_sla.minute, second=0, microsecond=0
            )
            if next_dt <= now:
                next_dt += timedelta(days=1)
            return next_dt.isoformat()
        return None

    @staticmethod
    def _calc_fresh_until(
        cron: list | None, sla: str | None, reference_time: datetime | None = None
    ) -> str | None:
        """Calculate the fresh_until timestamp based on CRON schedules or SLA.

        This function determines the next freshness deadline for a node with priority
        given to CRON schedules and then SLA.
        The logic is as follows:
            1.CRON schedules: It computes the earliest next cron run time.
            2.SLA configuration: It applies SLA rules (timedelta or wall-clock).

        Args:
            cron: List of cron schedules. Each item can be a dict with ``"cron"``
                and ``"timezone"`` keys, or a plain cron string.
            sla: SLA string (e.g. ``"24h"``, ``"10:00"``), or ``None``.
            reference_time: The UTC datetime to base calculations on. Defaults to
                ``datetime.now(timezone.utc)``.

        Returns:
            ISO-formatted fresh_until string, or ``None``.
        """
        logger.info("Calculating fresh_until ...")
        now = reference_time or datetime.now(timezone.utc)
        if cron:
            return StateHandler._calc_fresh_until_from_cron(cron, now)
        if sla is not None:
            return StateHandler._calc_fresh_until_from_sla(sla, now)
        logger.warning("Cannot calculate fresh_until. Setting fresh_until to None.")
        return None

    def build_node_state(  # noqa: PLR0913
        self,
        table_name: str,
        status: str,
        node_type: str,
        sla: str | None = None,
        owners: list[dict] | None = None,
        effective_source_data_slot: str | None = None,
        batch_id: int | None = None,
        cron: list | None = None,
        trigger_delay: int = 0,
        sla_breach_grace_period: int = 30,
        reference_time: datetime | None = None,
    ) -> dict:
        """Build the node state payload.

        Args:
            table_name: The dbt node name (model or source).
            status: Current run status (e.g. ``"success"``, ``"failed"``).
            node_type: The dbt node type (e.g. ``"model"``, ``"source"``).
            sla: Optional SLA string (e.g. ``"24h"``, ``"10:00"``).
            owners: Optional list of owner dicts.
            effective_source_data_slot: Optional effective source data slot.
            batch_id: Optional batch identifier.
            cron: Optional list of cron schedule dicts or strings.
            trigger_delay: Delay in minutes before triggering downstream nodes.
            sla_breach_grace_period: Grace period in minutes before an SLA breach.
            reference_time: Override for "now". Defaults to
                ``datetime.now(timezone.utc)``.

        Returns:
            Node state dict ready to be passed to ``StateStore.write``.
        """
        logger.info("Building node state payload ...")
        now = reference_time or datetime.now(timezone.utc)
        # fresh_until is only calculated on success; on failure it is preserved
        # by the StateStore._merge_node_state logic.
        fresh_until = (
            self._calc_fresh_until(cron, sla, now) if status == "success" else None
        )
        return {
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

    def update(self, node_state: dict) -> None:
        """Persist ``node_state`` to the store.

        Args:
            node_state: A node state dict as returned by ``build_node_state``.
        """
        logger.info(
            f"Writing state for node '{node_state.get('table_name')}' to store."
        )
        self.store.write(node_state=node_state)
