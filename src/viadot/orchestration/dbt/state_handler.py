"""State management logic for dbt dynamic orchestration."""

from datetime import datetime, time, timedelta, timezone
import logging
import re
from zoneinfo import ZoneInfo

from croniter import croniter
from dateutil.relativedelta import relativedelta

from viadot.orchestration.dbt.state_store import StateStore


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
        """Initialize StateHandler with the given StateStore instance."""
        self.store = store

    @staticmethod
    def _calc_fresh_until(
        node_type: str,
        schedules: list | None,
        sla: str | list[str] | None,
        reference_time: datetime | None = None,
    ) -> str | None:
        """Calculate the fresh_until timestamp based on node type.

        Sources use CRON schedules, while models use SLA rules (timedelta,
        wall-clock, cron string, or a list of cron strings).

        Args:
            node_type: The dbt node type (e.g. ``"source"`` or ``"model"``).
            schedules: List of Prefect schedules for sources. NOTE: currently, only
                cron schedules are supported. The list can contain dicts with a
                "cron" key or simple cron strings.
            sla: SLA value (e.g. ``"24h"``, ``"10:00"``, ``"0 10 * * *"``,
                or a list of cron expressions), or ``None``.
            reference_time: The UTC datetime to base calculations on. Defaults to
                ``datetime.now(timezone.utc)``.

        Returns:
            ISO-formatted fresh_until string, or ``None``.
        """
        logger.info("Calculating fresh_until ...")
        now = reference_time or datetime.now(timezone.utc)
        match node_type:
            case "source":
                if schedules:
                    return StateHandler._calc_fresh_until_from_crons(schedules, now)
                logger.warning(
                    "No cron schedule found for source node; cannot calculate "
                    "fresh_until. Setting fresh_until to None."
                )
                return None
            case "model":
                if sla:
                    return StateHandler._calc_fresh_until_from_sla(sla, now)
                logger.warning(
                    "No SLA found for model node; cannot calculate fresh_until. "
                    "Setting fresh_until to None."
                )
                return None
            case _:
                logger.warning(
                    f"Unknown node type '{node_type}'; cannot calculate fresh_until. "
                    "Setting fresh_until to None."
                )
                return None

    @staticmethod
    def _calc_fresh_until_from_crons(crons: list, now: datetime) -> str | None:
        """Return the earliest next cron run time as an ISO string, or None."""
        next_times = []
        for c in crons:
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
    def _calc_fresh_until_from_sla(sla: str | list, now: datetime) -> str | None:
        """Return the SLA-based fresh_until, or None if SLA is ignored."""
        if isinstance(sla, str) and sla.strip().lower() in ("ignored", "n/a"):
            return None

        parsed_sla = StateHandler._parse_sla(sla)

        # Cron-based SLA: calculate the next cron run time.
        if isinstance(parsed_sla, list):
            return StateHandler._calc_fresh_until_from_crons(parsed_sla, now)

        # Period-based SLA: add the timedelta or relativedelta to the current time.
        if isinstance(parsed_sla, timedelta | relativedelta):
            return (now + parsed_sla).isoformat()

        # Wall-clock SLA: use next occurrence of the wall-clock time
        next_dt = now.replace(
            hour=parsed_sla.hour, minute=parsed_sla.minute, second=0, microsecond=0
        )
        if next_dt <= now:
            next_dt += timedelta(days=1)
        return next_dt.isoformat()

    @staticmethod
    def _parse_sla(sla: str | list) -> timedelta | relativedelta | time | list:
        """Parse an SLA value.

        Parses SLA into one of:
        - ``timedelta`` (period)
        - ``relativedelta`` (calendar period)
        - ``time`` (wall-clock deadline)
        - ``list`` (normalized cron payload)

        Supported formats:
        - Hours:      ``"24 hours"``, ``"24h"``
        - Minutes:    ``"30 minutes"``, ``"30m"``
        - Days:       ``"7 days"``, ``"7d"``
        - Months:     ``"1 month"``, ``"2months"``, ``"1mo"``
        - Years:      ``"1 year"``, ``"2years"``, ``"1yr"``, ``"1y"``
        - Wall-clock: ``"10:00"``, ``"14:30"``
        - Cron:       ``"0 10 * * *"``, or a list of cron values
        """
        logger.info("Parsing SLA configuration...")

        if not isinstance(sla, str | list):
            msg = "SLA must be a string or a list of cron expressions."
            raise TypeError(msg)

        # Try each parser in order: cron, period, wall-clock. Return the first
        # successful parse.
        for parser in (
            StateHandler._parse_cron_sla,
            StateHandler._parse_period_sla,
            StateHandler._parse_wallclock_sla,
        ):
            parsed_sla = parser(sla)  # type: ignore
            if parsed_sla is not None:
                return parsed_sla

        raise SLAParseError(sla=sla)  # type: ignore

    @staticmethod
    def _parse_cron_sla(sla: str | list[str]) -> list[str] | None:
        """Parse and validate cron-based SLA values."""
        if isinstance(sla, str):
            sla_normalized = sla.strip()
            if croniter.is_valid(sla_normalized):
                return [sla_normalized]
            return None

        if isinstance(sla, list):
            return StateHandler._parse_cron_sla_list(sla)

        return None

    @staticmethod
    def _parse_cron_sla_list(sla: list) -> list:
        """Parse and validate cron-list SLA values."""
        normalized_crons = []
        for cron_item in sla:
            if isinstance(cron_item, str):
                cron_normalized = cron_item.strip()
                if not croniter.is_valid(cron_normalized):
                    raise SLAParseError(sla=cron_normalized)
                normalized_crons.append(cron_normalized)
                continue

            if isinstance(cron_item, dict):
                cron = cron_item.get("cron", "")
                cron_normalized = cron.strip()
                if (
                    not cron_normalized
                    or not isinstance(cron_normalized, str)
                    or not croniter.is_valid(cron_normalized)
                ):
                    raise SLAParseError(sla=cron_normalized)

                normalized_crons.append({**cron_item, "cron": cron_normalized})
                continue

            raise SLAParseError(sla=cron_item)

        return normalized_crons

    @staticmethod
    def _parse_period_sla(sla_str: str) -> timedelta | relativedelta | None:
        """Parse year/month/day/hour/minute SLA values."""
        # Years are matched before month/day/minute to avoid token overlap.
        yr = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(?:years?|yr?|y)", sla_str, re.IGNORECASE)
        if yr:
            return relativedelta(years=int(float(yr.group(1))))

        # Months are matched before minutes to avoid 'm' ambiguity.
        mo = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(?:months?|mo)", sla_str, re.IGNORECASE)
        if mo:
            return relativedelta(months=int(float(mo.group(1))))

        period_match = re.fullmatch(
            r"(\d+(?:\.\d+)?)\s*(hours?|hr?|minutes?|mins?|days?|[hmd])",
            sla_str,
            re.IGNORECASE,
        )
        if not period_match:
            return None

        value, unit = float(period_match.group(1)), period_match.group(2)[0].lower()
        if unit == "h":
            return timedelta(hours=value)
        if unit == "m":
            return timedelta(minutes=value)
        return timedelta(days=value)

    @staticmethod
    def _parse_wallclock_sla(sla_str: str) -> time | None:
        """Parse wall-clock SLA values in HH:MM format."""
        wc = re.fullmatch(r"(\d{1,2}):(\d{2})", sla_str)
        if wc:
            return time(hour=int(wc.group(1)), minute=int(wc.group(2)))
        return None

    def build_node_state(  # noqa: PLR0913
        self,
        node_name: str,
        status: str,
        node_type: str,
        sla: str | list[str] | None = None,
        owners: list[dict] | None = None,
        effective_source_data_slot: str | None = None,
        batch_id: int | None = None,
        schedules: list[str] | None = None,
        trigger_delay: int = 0,
        sla_breach_grace_period_minutes: int = 30,
        reference_time: datetime | None = None,
    ) -> dict:
        """Build the node state payload.

        Args:
            node_name: The dbt node name (model or source).
            status: Current run status (e.g. ``"success"``, ``"failed"``).
            node_type: The dbt node type (e.g. ``"model"``, ``"source"``).
            sla: Optional SLA value (e.g. ``"24h"``, ``"10:00"``,
                ``"0 10 * * *"``, or a list of cron expressions).
            owners: Optional list of owner dicts.
            effective_source_data_slot: Optional effective source data slot.
            batch_id: Optional batch identifier.
            schedules: Optional list of schedule dicts or strings.
            trigger_delay: Delay in minutes before triggering downstream nodes.
            sla_breach_grace_period_minutes: Grace period before an SLA breach.
            reference_time: Reference time for freshness calculations. Defaults to
                ``datetime.now(timezone.utc)``.

        Returns:
            Node state dict ready to be passed to ``StateStore.write``.
        """
        logger.info("Building node state payload ...")
        now = reference_time or datetime.now(timezone.utc)
        # fresh_until is only calculated on success; on failure it is preserved
        # by the StateStore._merge_node_state logic.
        fresh_until = (
            self._calc_fresh_until(node_type, schedules, sla, now)
            if status == "success"
            else None
        )
        return {
            "table_name": node_name,
            "node_type": node_type,
            "status": status,
            "last_refreshed_at": now.isoformat(),
            "fresh_until": fresh_until,
            "SLA": sla,
            "owners": owners,
            "effective_source_data_slot": effective_source_data_slot,
            "batch_id": batch_id,
            "schedules": schedules,
            "trigger_delay": trigger_delay,
            "sla_breach_grace_period": sla_breach_grace_period_minutes,
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


class SLAParseError(Exception):
    """Custom exception for SLA parsing errors."""

    def __init__(self, sla: str) -> None:
        """Initialize the SLAParseError with the SLA value."""
        message = f"Cannot parse SLA: {sla}."
        super().__init__(message)
