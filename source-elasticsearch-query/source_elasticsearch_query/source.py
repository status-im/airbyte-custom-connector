import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, List, Mapping, Tuple

import requests

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .client import (
    DEFAULT_CURSOR,
    fail,
    has_auth,
    headers,
    post_with_retry,
    query_body,
    search_url,
)
from .streams import DocumentsStream

logger = logging.getLogger("airbyte")


def _stream_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", (name or "").strip())
    cleaned = cleaned.strip("_") or "documents"
    if cleaned[0].isdigit():
        cleaned = f"q_{cleaned}"
    return cleaned


def configured_queries(config: Mapping[str, Any]) -> List[Tuple[str, str]]:
    """Return (stream_name, lucene_query) pairs. Each query gets its own cursor."""
    items = config.get("queries") or []
    pairs: List[Tuple[str, str]] = []
    seen = set()
    for i, item in enumerate(items):
        raw_name = item.get("name") or f"query_{i + 1}"
        query = (item.get("query") or "").strip()
        if not query:
            fail(f"Query '{raw_name}' is empty. An empty Lucene string matches the whole index.")
        name = _stream_name(raw_name)
        if name in seen:
            name = f"{name}_{i + 1}"
        seen.add(name)
        pairs.append((name, query))
    if not pairs:
        fail("At least one entry is required in queries.")
    return pairs


class SourceElasticsearchQuery(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            if not has_auth(config):
                return False, "Password did not reach the connector"
            url = search_url(config)
            cursor = config.get("cursor_field") or DEFAULT_CURSOR
            with requests.Session() as session:
                for name, query in configured_queries(config):
                    body = query_body(query, cursor, config["start_date"], 0, None)
                    body["size"] = 0
                    body["track_total_hits"] = True
                    resp = post_with_retry(
                        session,
                        url,
                        body,
                        headers(config),
                        config,
                        f"Elasticsearch check for query '{name}'",
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
            for name, query in configured_queries(config)
        ]
