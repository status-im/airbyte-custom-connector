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


def _load_schema(name: str) -> dict:
    with open(os.path.join(SCHEMAS_DIR, f"{name}.json"), "r") as f:
        return json.load(f)


def _github_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


class SearchCountsStream(Stream):
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
        headers = _github_headers(self._config["github_token"])
        queries = self._config["search_queries"]

        for i, sq in enumerate(queries):
            logger.info(f"Searching GitHub for query '{sq['name']}': {sq['query']}")
            resp = requests.get(
                GITHUB_SEARCH_URL,
                params={"q": sq["query"]},
                headers=headers,
            )
            resp.raise_for_status()
            data = resp.json()

            yield {
                "query_name": sq["name"],
                "query": sq["query"],
                "total_count": data["total_count"],
                "incomplete_results": data.get("incomplete_results", False),
            }

            if i < len(queries) - 1:
                time.sleep(6)


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
            return False, f"GitHub API returned status {resp.status_code}: {resp.text}"
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [SearchCountsStream(config)]
