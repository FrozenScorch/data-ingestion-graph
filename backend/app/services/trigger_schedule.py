"""Validation and timezone-aware next-time calculation for schedules."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, CroniterBadDateError, croniter

MIN_INTERVAL_SECONDS = 60
MAX_INTERVAL_SECONDS = 31_536_000


def validate_timezone(timezone_name: str) -> str:
    """Return a valid IANA timezone name or raise a stable validation error."""
    normalized = timezone_name.strip()
    if not normalized:
        raise ValueError("timezone must not be empty")
    try:
        ZoneInfo(normalized)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"Unknown timezone: {normalized}") from exc
    return normalized


def validate_cron_expression(expression: str) -> str:
    """Validate a conventional five-field cron expression (no seconds/year)."""
    normalized = " ".join(expression.strip().split())
    if len(normalized.split(" ")) != 5:
        raise ValueError("cron_expression must contain exactly five fields")
    if not croniter.is_valid(normalized, second_at_beginning=False):
        raise ValueError("cron_expression is invalid")
    return normalized


def validate_schedule_configuration(
    *,
    schedule_kind: str | None,
    interval_seconds: int | None,
    cron_expression: str | None,
    timezone_name: str,
) -> tuple[str, int | None, str | None, str]:
    """Validate and normalize the mutually exclusive schedule fields."""
    normalized_timezone = validate_timezone(timezone_name)
    if schedule_kind == "interval":
        if interval_seconds is None:
            raise ValueError("interval_seconds is required for an interval schedule")
        if not MIN_INTERVAL_SECONDS <= interval_seconds <= MAX_INTERVAL_SECONDS:
            raise ValueError(
                f"interval_seconds must be between {MIN_INTERVAL_SECONDS} "
                f"and {MAX_INTERVAL_SECONDS}"
            )
        if cron_expression is not None:
            raise ValueError("cron_expression is only valid for a cron schedule")
        return schedule_kind, interval_seconds, None, normalized_timezone
    if schedule_kind == "cron":
        if interval_seconds is not None:
            raise ValueError("interval_seconds is only valid for an interval schedule")
        if cron_expression is None:
            raise ValueError("cron_expression is required for a cron schedule")
        return (
            schedule_kind,
            None,
            validate_cron_expression(cron_expression),
            normalized_timezone,
        )
    raise ValueError("schedule_kind must be 'interval' or 'cron'")


def compute_next_run_at(
    *,
    schedule_kind: str,
    interval_seconds: int | None,
    cron_expression: str | None,
    timezone_name: str,
    now: datetime,
    previous_run_at: datetime | None = None,
) -> datetime:
    """Return the first scheduled instant strictly after ``now``.

    ``previous_run_at`` anchors interval cadence. If it is in the past, all
    missed occurrences are skipped instead of being replayed as a backlog.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    now = now.astimezone(UTC)
    kind, seconds, expression, normalized_timezone = validate_schedule_configuration(
        schedule_kind=schedule_kind,
        interval_seconds=interval_seconds,
        cron_expression=cron_expression,
        timezone_name=timezone_name,
    )
    if kind == "interval":
        assert seconds is not None
        anchor = previous_run_at
        if anchor is None:
            return now + timedelta(seconds=seconds)
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=UTC)
        anchor = anchor.astimezone(UTC)
        if anchor > now:
            return anchor
        missed_steps = int((now - anchor).total_seconds() // seconds) + 1
        return anchor + timedelta(seconds=missed_steps * seconds)

    assert expression is not None
    timezone = ZoneInfo(normalized_timezone)
    try:
        next_local = croniter(
            expression,
            now.astimezone(timezone),
            ret_type=datetime,
            second_at_beginning=False,
        ).get_next(datetime)
    except (CroniterBadCronError, CroniterBadDateError, ValueError) as exc:
        raise ValueError("cron_expression could not produce a next run") from exc
    if next_local.tzinfo is None:
        next_local = next_local.replace(tzinfo=timezone)
    return next_local.astimezone(UTC)
