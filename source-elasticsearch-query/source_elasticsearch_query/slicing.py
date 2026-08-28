from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

Window = Tuple[datetime, datetime]


def floor_to_day(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def floor_to_hour(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _windows(start: datetime, end: datetime, step: timedelta, align) -> List[Window]:
    if end <= start:
        return []
    windows: List[Window] = []
    edge = align(start)
    while edge < end:
        windows.append((edge, edge + step))
        edge += step
    return windows


def day_windows(start: datetime, end: datetime) -> List[Window]:
    return _windows(start, end, timedelta(days=1), floor_to_day)


def hour_windows(start: datetime, end: datetime) -> List[Window]:
    """Half-open UTC hour windows covering ``[start, end)``, aligned to the hour."""
    return _windows(start, end, timedelta(hours=1), floor_to_hour)


def index_names(
    pattern: Optional[str],
    start: datetime,
    end: datetime,
    neighbour_days: int = 0,
) -> List[str]:
    if not pattern or not str(pattern).strip() or end <= start:
        return []
    span = max(0, int(neighbour_days))
    first = floor_to_day(start) - timedelta(days=span)
    last = floor_to_day(end - timedelta(microseconds=1)) + timedelta(days=span)

    names: List[str] = []
    seen = set()
    day = first
    while day <= last:
        name = day.strftime(str(pattern))
        if name not in seen:
            seen.add(name)
            names.append(name)
        day += timedelta(days=1)
    return names


def target_indices(
    index: Optional[str],
    pattern: Optional[str],
    start: datetime,
    end: datetime,
    neighbour_days: int = 0,
) -> str:
    names = index_names(pattern, start, end, neighbour_days)
    if names:
        return ",".join(names)
    return str(index or "").strip() or "_all"
