import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

from airbyte_cdk.models import FailureType, SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.utils import AirbyteTracedException

logger = logging.getLogger("airbyte")

DEFAULT_PAGE_SIZE = 500
DEFAULT_CURSOR = "@timestamp"
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "hits.json")


def _fail(message: str, *, internal: Optional[str] = None, config_error: bool = True) -> None:
    raise AirbyteTracedException(
        message=message,
        internal_message=internal or message,
        failure_type=FailureType.config_error if config_error else FailureType.system_error,
    )


def _auth(config: Mapping[str, Any]):
    api_key = (config.get("api_key") or "").strip()
    if api_key:
        return None
    username = config.get("username")
    password = config.get("password")
    if username and password:
        return HTTPBasicAuth(username, password)
    return None


def _headers(config: Mapping[str, Any]) -> dict:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    api_key = (config.get("api_key") or "").strip()
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    return headers


def _endpoint(config: Mapping[str, Any]) -> str:
    return config["endpoint"].rstrip("/")


def _search_url(config: Mapping[str, Any]) -> str:
    return f"{_endpoint(config)}/{config['index']}/_search"


def _query_body(query: str, cursor_field: str, cursor_value: str, page_size: int, search_after: Optional[list]) -> dict:
    filters: List[dict] = [{"range": {cursor_field: {"gt": cursor_value}}}]
    if query.strip():
        filters.append({"query_string": {"query": query.strip()}})
    body: dict = {
        "size": page_size,
        "track_total_hits": False,
        "sort": [{cursor_field: "asc"}, {"_id": "asc"}],
        "query": {"bool": {"filter": filters}},
    }
    if search_after:
        body["search_after"] = search_after
    return body


class DocumentsStream(Stream, IncrementalMixin):
    """One row per Elasticsearch hit. ``_source`` fields are flattened onto the record."""

    name = "documents"
    primary_key = ["_index", "_id"]
    cursor_field = DEFAULT_CURSOR

    def get_json_schema(self) -> Mapping[str, Any]:
        # Stream names are config-defined, so every query stream shares this file.
        with open(SCHEMA_PATH) as f:
            return json.load(f)

    def __init__(self, config: Mapping[str, Any], query_name: str, query: str):
        super().__init__()
        self._config = config
        self._query_name = query_name
        self._query = query
        self.name = query_name
        self.cursor_field = config.get("cursor_field") or DEFAULT_CURSOR
        self._state: MutableMapping[str, Any] = {}

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = dict(value or {})

    def _cursor_floor(self, stream_state: Optional[Mapping[str, Any]]) -> str:
        state = stream_state or self._state or {}
        return state.get(self.cursor_field) or self._config["start_date"]

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
        query = self._query
        url = _search_url(config)
        auth = _auth(config)
        headers = _headers(config)

        search_after = None
        total = 0
        last_cursor = floor
        while True:
            body = _query_body(query, cursor_name, floor, page_size, search_after)
            logger.info(
                f"Search stream={self.name} cursor_gt={floor} search_after={search_after}"
            )
            try:
                resp = requests.post(url, json=body, auth=auth, headers=headers, timeout=60)
            except requests.RequestException as e:
                _fail(f"Elasticsearch request failed: {e}", config_error=False)
            if resp.status_code in (400, 401, 403, 404):
                _fail(
                    f"Elasticsearch HTTP {resp.status_code} for {url}: {resp.text[:500]}",
                    config_error=True,
                )
            resp.raise_for_status()
            payload = resp.json()
            hits = ((payload.get("hits") or {}).get("hits")) or []
            if not hits:
                break
            for hit in hits:
                source = hit.get("_source") or {}
                record = {
                    "_id": hit.get("_id"),
                    "_index": hit.get("_index"),
                    **source,
                    "query_name": self._query_name,
                }
                yield record
                value = record.get(cursor_name)
                if value and str(value) > str(last_cursor):
                    last_cursor = str(value)
                total += 1
            if len(hits) < page_size:
                break
            search_after = hits[-1].get("sort")
            if not search_after:
                _fail(
                    "Elasticsearch hit is missing sort values; cannot page with search_after.",
                    config_error=False,
                )

        if last_cursor and last_cursor != (self._state.get(cursor_name) or ""):
            self._state = {cursor_name: last_cursor}
        logger.info(f"Stream {self.name} emitted {total} documents (cursor={self._state.get(cursor_name)})")


def _stream_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", (name or "").strip())
    cleaned = cleaned.strip("_") or "documents"
    if cleaned[0].isdigit():
        cleaned = f"q_{cleaned}"
    return cleaned


def _configured_queries(config: Mapping[str, Any]) -> List[Tuple[str, str]]:
    """Return (stream_name, lucene_query) pairs. Each query gets its own cursor."""
    items = config.get("queries") or []
    pairs: List[Tuple[str, str]] = []
    seen = set()
    for i, item in enumerate(items):
        if isinstance(item, str):
            raw_name, query = f"query_{i + 1}", item
        else:
            raw_name = item.get("name") or f"query_{i + 1}"
            query = item.get("query") or ""
        name = _stream_name(raw_name)
        if name in seen:
            name = f"{name}_{i + 1}"
        seen.add(name)
        pairs.append((name, query))
    if not pairs:
        pairs.append(("documents", config.get("query") or ""))
    return pairs


class SourceElasticsearchQuery(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            url = _search_url(config)
            cursor = config.get("cursor_field") or DEFAULT_CURSOR
            queries = _configured_queries(config)
            for name, query in queries:
                body = _query_body(query, cursor, config["start_date"], 0, None)
                body["size"] = 0
                body["track_total_hits"] = True
                resp = requests.post(
                    url,
                    json=body,
                    auth=_auth(config),
                    headers=_headers(config),
                    timeout=30,
                )
                if resp.status_code != 200:
                    return False, f"HTTP {resp.status_code} for query '{name}' {url}: {resp.text[:500]}"
                hits = (resp.json().get("hits") or {}).get("total")
                logger.info(f"Elasticsearch check ok query={name} hits.total={hits}")
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        if not config.get("start_date"):
            config = dict(config)
            config["start_date"] = (
                datetime.now(timezone.utc) - timedelta(days=2)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        return [
            DocumentsStream(config, name, query)
            for name, query in _configured_queries(config)
        ]
