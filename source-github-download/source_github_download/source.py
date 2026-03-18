import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode

logger = logging.getLogger("airbyte")

GITHUB_API_BASE = "https://api.github.com"


def fetch_releases(repo: str) -> List[dict]:
    """Fetch all releases for a repo, handling pagination."""
    url = f"{GITHUB_API_BASE}/repos/{repo}/releases"
    all_releases = []

    while url:
        response = requests.get(url, headers={"Accept": "application/vnd.github+json"})

        if response.status_code == 403 and "rate limit" in response.text.lower():
            reset_at = int(response.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_at - int(time.time()), 1)
            logger.warning(f"Rate limited. Waiting {wait}s before retrying.")
            time.sleep(wait)
            continue

        response.raise_for_status()
        all_releases.extend(response.json())

        # Follow pagination via Link header
        link_header = response.headers.get("Link", "")
        url = None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")

    return all_releases


class ReleaseDownloadStream(Stream):

    primary_key = None

    def __init__(self, config: Mapping[str, Any]):
        self.repositories = config["repositories"]

    @property
    def name(self) -> str:
        return "release_download"

    def get_json_schema(self) -> Mapping[str, Any]:
        schema_path = os.path.join(os.path.dirname(__file__), "schemas", "release_download.json")
        with open(schema_path, "r") as f:
            return json.load(f)

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for repo in self.repositories:
            yield {"repository": repo}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Dict[str, Any]] = None,
        stream_state: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        repo = stream_slice["repository"]
        logger.info(f"Fetching releases for {repo}")
        fetched_at = datetime.now(timezone.utc).isoformat()

        try:
            releases = fetch_releases(repo)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to fetch releases for {repo}: {e}")
            return

        for release in releases:
            assets = release.get("assets", [])
            if not assets:
                continue

            for asset in assets:
                yield {
                    "repository": repo,
                    "release_id": release.get("id"),
                    "release_name": release.get("name"),
                    "tag_name": release.get("tag_name"),
                    "published_at": release.get("published_at"),
                    "prerelease": release.get("prerelease"),
                    "asset_name": asset.get("name"),
                    "download_count": asset.get("download_count"),
                    "asset_size": asset.get("size"),
                    "content_type": asset.get("content_type"),
                    "fetched_at": fetched_at,
                }


class SourceGithubDownload(AbstractSource):

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        repositories = config.get("repositories", [])
        if not repositories:
            return False, "No repositories configured"

        repo = repositories[0]
        try:
            response = requests.get(
                f"{GITHUB_API_BASE}/repos/{repo}/releases",
                headers={"Accept": "application/vnd.github+json"},
            )
            if response.status_code == 200:
                return True, None
            return False, f"GitHub API returned status {response.status_code} for {repo}: {response.text}"
        except Exception as e:
            return False, f"Error connecting to GitHub API: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [ReleaseDownloadStream(config)]
