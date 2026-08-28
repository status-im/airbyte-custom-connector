import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

from .client import (
    DEFAULT_CURSOR,
    DEFAULT_INGESTION_LAG_SECONDS,
    DEFAULT_LOOKBACK_HOURS,
    DEFAULT_NEIGHBOUR_DAYS,
    DEFAULT_SUBDIVIDE_THRESHOLD,
    base_url,
    build_query,
    close_pit,
    close_scroll,
    count_documents,
    fail,
    iso_z,
    keep_alive,
    next_scroll,
    open_pit,
    open_scroll,
    page_size,
    parse_timestamp,
    request_with_retry,
    scroll_body,
    search_body,
)
from .slicing import day_windows, hour_windows, target_indices

logger = logging.getLogger("airbyte")

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "document.json")


class DocumentsStream(Stream, IncrementalMixin):

    name = "documents"
    primary_key = ["_index", "_id"]
    cursor_field = DEFAULT_CURSOR

    def __init__(
        self,
        config: Mapping[str, Any],
        query_name: str,
        query: str,
        filters: Optional[List[dict]] = None,
    ):
        super().__init__()
        self._config = config
        self._query_name = query_name
        self._query = query or ""
        self._filters = list(filters or [])
        self.name = query_name
        self.cursor_field = config.get("cursor_field") or DEFAULT_CURSOR
        self._state: MutableMapping[str, Any] = {}

        self._page_size = page_size(config)
        self._source_fields = [
            str(field).strip() for field in (config.get("source_fields") or []) if str(field).strip()
        ]
        self._assert_counts = config.get("assert_counts", True) is not False
        self._pagination = str(config.get("pagination") or "auto").strip().lower()
        self._pit_available = self._pagination in ("auto", "pit")

    def get_json_schema(self) -> Mapping[str, Any]:
        with open(SCHEMA_PATH) as f:
            schema = json.load(f)
        schema["properties"].setdefault(self.cursor_field, {"type": ["null", "string"]})
        extra = list(self._config.get("extra_fields") or []) + self._source_fields
        for field in extra:
            name = str(field).strip()
            if name:
                schema["properties"].setdefault(name, {"type": ["null", "string"]})
        return schema

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = dict(value or {})

    def _advance_cursor(self, slice_end: datetime) -> None:
        current = self._state.get(self.cursor_field)
        if current is None or parse_timestamp(current) < slice_end:
            self._state = {self.cursor_field: iso_z(slice_end)}

    def _floor(self, sync_mode: SyncMode, stream_state: Optional[Mapping[str, Any]]) -> datetime:
        start = parse_timestamp(self._config["start_date"])
        if sync_mode != SyncMode.incremental:
            return start
        raw = (stream_state or self._state or {}).get(self.cursor_field)
        if not raw:
            return start
        lookback = self._config.get("lookback_hours")
        hours = DEFAULT_LOOKBACK_HOURS if lookback is None else float(lookback)
        resumed = parse_timestamp(raw) - timedelta(hours=max(0.0, hours))
        return max(start, resumed)

    def _ceiling(self) -> datetime:
        lag = self._config.get("ingestion_lag_seconds")
        seconds = DEFAULT_INGESTION_LAG_SECONDS if lag is None else int(lag)
        return datetime.now(timezone.utc) - timedelta(seconds=max(0, seconds))

    def _neighbour_days(self) -> int:
        value = self._config.get("index_neighbour_days")
        return DEFAULT_NEIGHBOUR_DAYS if value is None else max(0, int(value))

    def _slice(self, start: datetime, end: datetime, expected: Optional[int] = None) -> Dict[str, Any]:
        return {
            "start": iso_z(start),
            "end": iso_z(end),
            "indices": target_indices(
                self._config.get("index"),
                self._config.get("index_date_pattern"),
                start,
                end,
                self._neighbour_days(),
            ),
            "label": f"{iso_z(start)}..{iso_z(end)}",
            "expected": expected,
        }

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        floor = self._floor(sync_mode, stream_state)
        ceiling = self._ceiling()
        if ceiling <= floor:
            logger.info(
                f"Stream {self.name}: nothing to read, cursor {iso_z(floor)} is already at or past "
                f"{iso_z(ceiling)}."
            )
            return

        granularity = str(self._config.get("slice_granularity") or "auto").strip().lower()
        if granularity not in ("auto", "day", "hour"):
            fail(f"slice_granularity must be one of auto, day or hour; got {granularity!r}.")
        threshold = int(self._config.get("subdivide_threshold") or DEFAULT_SUBDIVIDE_THRESHOLD)
        days = day_windows(floor, ceiling)
        logger.info(
            f"Stream {self.name}: {len(days)} days to read, {iso_z(floor)} to {iso_z(ceiling)}, "
            f"granularity={granularity}."
        )

        session = requests.Session()
        try:
            for day_start, day_edge in days:
                day_end = min(day_edge, ceiling)
                if granularity == "hour":
                    yield from self._hourly(day_start, day_end)
                    continue
                if granularity == "day":
                    yield self._slice(day_start, day_end)
                    continue

                # auto: one _count per day decides whether the day needs hourly slices.
                candidate = self._slice(day_start, day_end)
                expected = count_documents(
                    session,
                    self._config,
                    candidate["indices"],
                    self._query_for(day_start, day_end),
                    f"_count for stream {self.name} day {day_start:%Y-%m-%d}",
                )
                if expected > threshold:
                    logger.info(
                        f"Stream {self.name}: {day_start:%Y-%m-%d} has {expected} matches, above "
                        f"subdivide_threshold {threshold}; splitting into hourly slices."
                    )
                    yield from self._hourly(day_start, day_end)
                else:
                    candidate["expected"] = expected
                    yield candidate
        finally:
            session.close()

    def _hourly(self, start: datetime, end: datetime) -> Iterable[Mapping[str, Any]]:
        for hour_start, hour_edge in hour_windows(start, end):
            yield self._slice(hour_start, min(hour_edge, end))

    def _query_for(self, start: datetime, end: datetime) -> dict:
        return build_query(
            self.cursor_field,
            iso_z(start),
            iso_z(end),
            query_string=self._query,
            filters=self._filters,
        )

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        if not stream_slice:
            return

        slice_start = parse_timestamp(stream_slice["start"])
        slice_end = parse_timestamp(stream_slice["end"])
        indices = stream_slice["indices"]
        label = stream_slice["label"]
        query = self._query_for(slice_start, slice_end)
        context = f"stream {self.name} slice {label}"

        session = requests.Session()
        try:
            expected = stream_slice.get("expected")
            if expected is None:
                expected = count_documents(
                    session, self._config, indices, query, f"_count for {context}"
                )

            emitted = 0
            if expected:
                for hit in self._walk(session, indices, query, context):
                    yield self._record(hit)
                    emitted += 1
            else:
                logger.info(f"{context}: _count is 0 across {indices}; nothing to read.")

            self._verify(context, int(expected), emitted)
            logger.info(f"{context}: expected={expected} emitted={emitted} indices={indices}")
            self._advance_cursor(slice_end)
        finally:
            session.close()

    def _verify(self, context: str, expected: int, emitted: int) -> None:
        if emitted < expected:
            message = (
                f"{context}: _count reported {expected} matching documents but the connector "
                f"emitted {emitted} ({expected - emitted} missing). Refusing to advance the cursor "
                "past unread data."
            )
            if self._assert_counts:
                fail(message, config_error=False)
            logger.error(f"{message} assert_counts is disabled, so the sync continues.")
        elif emitted > expected:
            logger.warning(
                f"{context}: emitted {emitted} documents but _count reported {expected}. Documents "
                "were indexed into this window after the count ran; the _id primary key dedupes "
                "them at the destination."
            )

    def _walk(
        self, session: requests.Session, indices: str, query: dict, context: str
    ) -> Iterable[Mapping[str, Any]]:
        if self._pit_available:
            pit_id = open_pit(session, self._config, indices, f"_pit for {context}")
            if pit_id:
                yield from self._walk_pit(session, pit_id, query, context)
                return
            if self._pagination == "pit":
                fail(
                    f"{context}: pagination is set to 'pit' but this cluster has no point-in-time "
                    "API. Set pagination to 'scroll' or 'auto'.",
                    config_error=True,
                )
            self._pit_available = False
        yield from self._walk_scroll(session, indices, query, context)

    def _walk_pit(
        self, session: requests.Session, pit_id: str, query: dict, context: str
    ) -> Iterable[Mapping[str, Any]]:
        url = f"{base_url(self._config)}/_search"
        alive = keep_alive(self._config)
        search_after: Optional[list] = None
        seen_boundaries = set()
        page = 0
        try:
            while True:
                page += 1
                body = search_body(
                    self.cursor_field,
                    query,
                    self._page_size,
                    pit={"id": pit_id, "keep_alive": alive},
                    search_after=search_after,
                    source_fields=self._source_fields,
                )
                _, payload = request_with_retry(
                    session, "POST", url, self._config, f"{context} page {page}", body=body
                )
                payload = payload or {}
                # The PIT id can be refreshed by the cluster; the next page must use the latest.
                pit_id = payload.get("pit_id") or pit_id
                hits = ((payload.get("hits") or {}).get("hits")) or []
                if not hits:
                    break
                for hit in hits:
                    yield hit
                search_after = hits[-1].get("sort")
                if not search_after:
                    fail(
                        f"{context}: the last hit of page {page} carries no sort values, so "
                        "search_after cannot continue. Refusing to skip the rest of the slice.",
                        config_error=False,
                    )
                boundary = json.dumps(search_after, sort_keys=True, default=str)
                if boundary in seen_boundaries:
                    fail(
                        f"{context}: search_after did not advance past {boundary} on page {page}; "
                        "stopping to avoid an endless loop.",
                        config_error=False,
                    )
                seen_boundaries.add(boundary)
        finally:
            close_pit(session, self._config, pit_id, context)

    def _walk_scroll(
        self, session: requests.Session, indices: str, query: dict, context: str
    ) -> Iterable[Mapping[str, Any]]:
        payload = open_scroll(
            session,
            self._config,
            indices,
            scroll_body(query, self._page_size, source_fields=self._source_fields),
            f"{context} scroll page 1",
        )
        scroll_id = payload.get("_scroll_id")
        page = 1
        try:
            while True:
                hits = ((payload.get("hits") or {}).get("hits")) or []
                if not hits:
                    break
                for hit in hits:
                    yield hit
                if not scroll_id:
                    fail(
                        f"{context}: Elasticsearch returned no _scroll_id on page {page}, so the "
                        "rest of the slice cannot be read.",
                        config_error=False,
                    )
                page += 1
                payload = next_scroll(
                    session, self._config, scroll_id, f"{context} scroll page {page}"
                )
                scroll_id = payload.get("_scroll_id") or scroll_id
        finally:
            if scroll_id:
                close_scroll(session, self._config, scroll_id, context)

    def _record(self, hit: Mapping[str, Any]) -> dict:
        source = hit.get("_source") or {}
        return {
            "_id": hit.get("_id"),
            "_index": hit.get("_index"),
            **source,
            "query_name": self._query_name,
        }
