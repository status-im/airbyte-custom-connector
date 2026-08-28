import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Tuple

import requests

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .client import (
    DEFAULT_CURSOR,
    DEFAULT_NEIGHBOUR_DAYS,
    build_query,
    close_pit,
    count_documents,
    fail,
    has_auth,
    iso_z,
    open_pit,
    parse_timestamp,
    phrase_filters,
)
from .slicing import target_indices
from .streams import DocumentsStream

logger = logging.getLogger("airbyte")


def _stream_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", (name or "").strip())
    cleaned = cleaned.strip("_") or "documents"
    if cleaned[0].isdigit():
        cleaned = f"q_{cleaned}"
    return cleaned


def configured_queries(config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Return one ``{name, query, filters}`` spec per stream. Each stream gets its own cursor."""
    common = phrase_filters(config.get("common_filters"), "common_filters")
    items = config.get("queries") or []
    specs: List[Dict[str, Any]] = []
    seen = set()
    for i, item in enumerate(items):
        raw_name = item.get("name") or f"query_{i + 1}"
        query = (item.get("query") or "").strip()
        filters = common + phrase_filters(item.get("filters"), f"queries[{i}].filters")
        if not query and not filters:
            fail(
                f"Query '{raw_name}' has neither 'query' nor 'filters'. That would match the "
                "whole index."
            )
        name = _stream_name(raw_name)
        if name in seen:
            name = f"{name}_{i + 1}"
        seen.add(name)
        specs.append({"name": name, "query": query, "filters": filters})
    if not specs:
        fail("At least one entry is required in queries.")
    return specs


class SourceElasticsearchQuery(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            if not has_auth(config):
                return False, "Password did not reach the connector"

            cursor = config.get("cursor_field") or DEFAULT_CURSOR
            neighbours = config.get("index_neighbour_days")
            neighbours = DEFAULT_NEIGHBOUR_DAYS if neighbours is None else max(0, int(neighbours))

            end = datetime.now(timezone.utc)
            start = max(parse_timestamp(config["start_date"]), end - timedelta(days=2))
            if start >= end:
                start = end - timedelta(days=2)
            indices = target_indices(
                config.get("index"), config.get("index_date_pattern"), start, end, neighbours
            )
            logger.info(f"Elasticsearch check target indices: {indices}")

            with requests.Session() as session:
                pit_id = open_pit(session, config, indices, "Elasticsearch check: _pit")
                if pit_id:
                    close_pit(session, config, pit_id, "Elasticsearch check")
                    logger.info("Point-in-time is available; slices page with pit + search_after.")
                elif str(config.get("pagination") or "auto").strip().lower() == "pit":
                    return False, (
                        "pagination is set to 'pit' but this cluster has no point-in-time API. "
                        "Use 'scroll' or 'auto'."
                    )
                else:
                    logger.info("Point-in-time is unavailable; slices page with the scroll API.")

                for spec in configured_queries(config):
                    query = build_query(
                        cursor,
                        iso_z(start),
                        iso_z(end),
                        query_string=spec["query"],
                        filters=spec["filters"],
                    )
                    matches = count_documents(
                        session,
                        config,
                        indices,
                        query,
                        f"Elasticsearch check: _count for query '{spec['name']}'",
                    )
                    logger.info(
                        f"Elasticsearch check ok query={spec['name']} "
                        f"matches_last_2_days={matches}"
                    )
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        if not config.get("start_date"):
            config = dict(config)
            config["start_date"] = iso_z(datetime.now(timezone.utc) - timedelta(days=2))
        return [
            DocumentsStream(config, spec["name"], spec["query"], spec["filters"])
            for spec in configured_queries(config)
        ]
