import json
import logging
import os
import time
from typing import Any, Iterable, List, Mapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

logger = logging.getLogger("airbyte")

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")
GITHUB_SEARCH_URL = "https://api.github.com/search/code"

REQUEST_SLEEP_SECONDS = 6
MAX_RESULTS_PER_QUERY = 1000
PER_PAGE = 100

RATE_LIMIT_MAX_RETRIES = 5
RATE_LIMIT_DEFAULT_WAIT_SECONDS = 60
RATE_LIMIT_MAX_WAIT_SECONDS = 300


def _load_schema(name: str) -> dict:
    with open(os.path.join(SCHEMAS_DIR, f"{name}.json"), "r") as f:
        return json.load(f)


def _github_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


def _is_rate_limited(resp: requests.Response) -> bool:
    """GitHub signals both primary and secondary rate limits with 403/429."""
    if resp.status_code not in (403, 429):
        return False
    if resp.headers.get("Retry-After"):
        return True
    if resp.headers.get("X-RateLimit-Remaining") == "0":
        return True
    return "rate limit" in resp.text.lower()


def _rate_limit_wait_seconds(resp: requests.Response, attempt: int) -> int:
    """Follow GitHub's documented backoff guidance for rate-limit responses."""
    retry_after = resp.headers.get("Retry-After", "")
    if retry_after.isdigit():
        wait = int(retry_after) + 1
    elif resp.headers.get("X-RateLimit-Remaining") == "0" and resp.headers.get(
        "X-RateLimit-Reset", ""
    ).isdigit():
        reset_at = int(resp.headers["X-RateLimit-Reset"])
        wait = max(reset_at - int(time.time()), 1) + 1
    else:
        # No explicit window: wait at least a minute, then back off exponentially.
        wait = RATE_LIMIT_DEFAULT_WAIT_SECONDS * (2 ** attempt)
    return min(wait, RATE_LIMIT_MAX_WAIT_SECONDS)


def _search_page(token: str, query: str, page: int, per_page: int) -> dict:
    resp = None
    for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
        resp = requests.get(
            GITHUB_SEARCH_URL,
            params={"q": query, "page": page, "per_page": per_page},
            headers=_github_headers(token),
        )
        if _is_rate_limited(resp) and attempt < RATE_LIMIT_MAX_RETRIES:
            wait = _rate_limit_wait_seconds(resp, attempt)
            logger.warning(
                f"GitHub rate limit hit (status {resp.status_code}) on page {page}; "
                f"waiting {wait}s before retry {attempt + 1}/{RATE_LIMIT_MAX_RETRIES}."
            )
            time.sleep(wait)
            continue
        break

    resp.raise_for_status()
    return resp.json()


class SearchCountsStream(Stream):
    """One row per query with aggregate total_count."""

    primary_key = None
    name = "search_counts"

    def __init__(self, config: Mapping[str, Any]):
        self._config = config

    def get_json_schema(self) -> Mapping[str, Any]:
        return _load_schema(self.name)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        token = self._config["github_token"]
        queries = self._config["search_queries"]

        for i, sq in enumerate(queries):
            logger.info(f"Searching GitHub (count) for '{sq['name']}': {sq['query']}")
            data = _search_page(token, sq["query"], page=1, per_page=1)

            yield {
                "query_name": sq["name"],
                "query": sq["query"],
                "total_count": data["total_count"],
                "incomplete_results": data.get("incomplete_results", False),
            }

            if i < len(queries) - 1:
                time.sleep(REQUEST_SLEEP_SECONDS)


class SearchItemsStream(Stream):
    """One row per search result item, with repo/owner metadata flattened."""

    primary_key = None
    name = "search_items"

    def __init__(self, config: Mapping[str, Any]):
        self._config = config

    def get_json_schema(self) -> Mapping[str, Any]:
        return _load_schema(self.name)

    @staticmethod
    def _flatten_item(query_name: str, query: str, item: Mapping[str, Any]) -> Mapping[str, Any]:
        repo = item.get("repository") or {}
        owner = repo.get("owner") or {}
        return {
            "query_name": query_name,
            "query": query,
            "name": item.get("name"),
            "path": item.get("path"),
            "html_url": item.get("html_url"),
            "score": item.get("score"),
            "repository_id": repo.get("id"),
            "repository_name": repo.get("name"),
            "repository_full_name": repo.get("full_name"),
            "repository_owner_login": owner.get("login"),
            "repository_owner_id": owner.get("id"),
            "repository_owner_type": owner.get("type"),
            "repository_private": repo.get("private"),
            "repository_fork": repo.get("fork"),
            "repository_html_url": repo.get("html_url"),
            "repository_description": repo.get("description"),
        }

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        token = self._config["github_token"]
        queries = self._config["search_queries"]

        first_request = True
        for sq in queries:
            query_name = sq["name"]
            query = sq["query"]
            logger.info(f"Searching GitHub (items) for '{query_name}': {query}")

            fetched = 0
            page = 1
            while fetched < MAX_RESULTS_PER_QUERY:
                if not first_request:
                    time.sleep(REQUEST_SLEEP_SECONDS)
                first_request = False

                data = _search_page(token, query, page=page, per_page=PER_PAGE)
                items = data.get("items") or []
                if not items:
                    break

                for item in items:
                    yield self._flatten_item(query_name, query, item)

                fetched += len(items)
                total = data.get("total_count", 0)
                if fetched >= total or len(items) < PER_PAGE:
                    break
                page += 1


class SourceGithubSearchCount(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            resp = requests.get(
                GITHUB_SEARCH_URL,
                params={"q": "test"},
                headers=_github_headers(config["github_token"]),
            )
            if resp.status_code == 200:
                return True, None
            if resp.status_code == 401:
                return False, f"GitHub authentication failed: {resp.text}"
            if _is_rate_limited(resp):
                logger.warning(
                    "GitHub rate limit hit during connection check; "
                    "treating credentials as valid."
                )
                return True, None
            return False, f"GitHub API returned status {resp.status_code}: {resp.text}"
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            SearchCountsStream(config),
            SearchItemsStream(config),
        ]
