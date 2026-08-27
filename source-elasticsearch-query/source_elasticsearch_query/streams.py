import json
import logging
import os
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

from .client import (
    DEFAULT_CURSOR,
    DEFAULT_PAGE_SIZE,
    fail,
    headers,
    post_with_retry,
    query_body,
    search_url,
)

logger = logging.getLogger("airbyte")

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "document.json")


class DocumentsStream(Stream, IncrementalMixin):
    """One row per Elasticsearch document. ``_source`` fields are flattened onto the record."""

    name = "documents"
    primary_key = ["_index", "_id"]
    cursor_field = DEFAULT_CURSOR

    def __init__(self, config: Mapping[str, Any], query_name: str, query: str):
        super().__init__()
        self._config = config
        self._query_name = query_name
        self._query = query
        self.name = query_name
        self.cursor_field = config.get("cursor_field") or DEFAULT_CURSOR
        self._state: MutableMapping[str, Any] = {}

    def get_json_schema(self) -> Mapping[str, Any]:
        with open(SCHEMA_PATH) as f:
            schema = json.load(f)
        schema["properties"].setdefault(self.cursor_field, {"type": ["null", "string"]})
        for field in self._config.get("extra_fields") or []:
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

    def _cursor_floor(self, stream_state: Optional[Mapping[str, Any]]) -> str:
        state = stream_state or self._state or {}
        return state.get(self.cursor_field) or self._config["start_date"]

    def _set_cursor(self, value: Optional[str]) -> None:
        if value and value != self._state.get(self.cursor_field):
            self._state = {self.cursor_field: value}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        config = self._config
        cursor_name = self.cursor_field
        floor = self._cursor_floor(stream_state)
        page_size = int(config.get("page_size") or DEFAULT_PAGE_SIZE)
        url = search_url(config)
        request_headers = headers(config)

        search_after = None
        total = 0
        highest_cursor = floor
        safe_cursor = floor
        session = requests.Session()
        try:
            while True:
                body = query_body(self._query, cursor_name, floor, page_size, search_after)
                logger.info(
                    f"Search stream={self.name} cursor_gt={floor} search_after={search_after}"
                )
                resp = post_with_retry(
                    session,
                    url,
                    body,
                    request_headers,
                    config,
                    f"Elasticsearch search for stream {self.name}",
                )
                if resp.status_code in (400, 401, 403, 404):
                    fail(
                        f"Elasticsearch HTTP {resp.status_code} for {url}: {resp.text[:500]}",
                        config_error=True,
                    )
                resp.raise_for_status()
                hits = ((resp.json().get("hits") or {}).get("hits")) or []
                if not hits:
                    break
                for hit in hits:
                    record = self._record(hit)
                    yield record
                    value = record.get(cursor_name)
                    if value and str(value) > str(highest_cursor):
                        safe_cursor = highest_cursor
                        highest_cursor = str(value)
                    total += 1
                self._set_cursor(safe_cursor)
                if len(hits) < page_size:
                    break
                search_after = hits[-1].get("sort")
                if not search_after:
                    fail(
                        "Elasticsearch hit is missing sort values; cannot page with search_after.",
                        config_error=False,
                    )
        finally:
            session.close()

        self._set_cursor(highest_cursor)
        logger.info(
            f"Stream {self.name} emitted {total} documents (cursor={self._state.get(cursor_name)})"
        )

    def _record(self, hit: Mapping[str, Any]) -> dict:
        source = hit.get("_source") or {}
        return {
            "_id": hit.get("_id"),
            "_index": hit.get("_index"),
            **source,
            "query_name": self._query_name,
        }
