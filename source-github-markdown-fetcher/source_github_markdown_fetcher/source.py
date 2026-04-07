import re
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


class GithubRepoContents(HttpStream):
    """Base stream that fetches markdown files from a GitHub repo directory and delegates parsing to subclasses."""

    url_base = "https://api.github.com/"
    primary_key = "number"

    # Subclasses must set these
    file_prefix: str = ""
    skip_prefix: str = ""

    def __init__(self, repo: str, path: str, github_token: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.repo = repo
        self.repo_path = path
        self.github_token = github_token

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.github_token:
            headers["Authorization"] = f"token {self.github_token}"
        return headers

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, **kwargs) -> str:
        return f"repos/{self.repo}/contents/{self.repo_path}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        files = response.json()
        if not isinstance(files, list):
            return

        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.github_token:
            headers["Authorization"] = f"token {self.github_token}"

        md_files = [
            f for f in files
            if f["name"].endswith(".md")
            and f["name"].startswith(self.file_prefix)
            and not f["name"].startswith(self.skip_prefix)
        ]

        for f in md_files:
            raw_resp = requests.get(f["download_url"], headers=headers, timeout=30)
            raw_resp.raise_for_status()
            parsed = self.parse_markdown(raw_resp.text, f["name"], f["html_url"])
            if parsed:
                yield parsed

    def parse_markdown(self, raw: str, filename: str, html_url: str) -> Optional[dict]:
        raise NotImplementedError


class Rfps(GithubRepoContents):
    """Stream that fetches RFP markdown files from GitHub and parses them into structured records."""

    file_prefix = "RFP-"
    skip_prefix = "RFP-000"

    def parse_markdown(self, raw: str, filename: str, html_url: str) -> Optional[dict]:
        number_m = re.search(r"^(RFP-\d+)", filename)
        number = number_m.group(1) if number_m else ""
        if not number or number == "RFP-000":
            return None

        h1 = re.search(r"^#\s+(.+)", raw, re.MULTILINE)
        title = re.sub(r"^RFP-\d+[:\s\-\u2014]+", "", h1.group(1)).strip() if h1 else filename

        category_m = (
            re.search(r"\*\*Category\*\*[:\s]*(.+)", raw, re.IGNORECASE)
            or re.search(r"Category[:\s|]*([^\n|]+)", raw, re.IGNORECASE)
        )
        category = re.sub(r"[`*]", "", category_m.group(1)).strip() if category_m else ""

        status_m = (
            re.search(r"\*\*Status\*\*[:\s]*(.+)", raw, re.IGNORECASE)
            or re.search(r"Status[:\s|]*([^\n|]+)", raw, re.IGNORECASE)
        )
        status = re.sub(r"[`*]", "", status_m.group(1)).strip() if status_m else "open"

        if "draft" in status.lower():
            return None

        tier_m = (
            re.search(r"\*\*Tier\*\*[:\s]*(.+)", raw, re.IGNORECASE)
            or re.search(r"Tier[:\s|]*([^\n|]+)", raw, re.IGNORECASE)
        )
        tier = re.sub(r"[`*]", "", tier_m.group(1)).strip() if tier_m else ""

        summary = ""
        ov_m = re.search(r"##\s*(?:\U0001f9ed\s*)?Overview\s*\n+([\s\S]*?)(?=\n##)", raw, re.IGNORECASE)
        if ov_m:
            lines = [line.strip() for line in ov_m.group(1).split("\n") if line.strip()]
            summary = " ".join(lines)[:200]

        if not summary:
            lines = [
                line.strip()
                for line in raw.split("\n")
                if line.strip()
                and not line.startswith("#")
                and not line.startswith("|")
                and not line.startswith("---")
                and not line.startswith("**")
            ]
            summary = " ".join(lines[:3])[:200]

        return {
            "number": number,
            "title": title,
            "category": category,
            "summary": summary,
            "github_url": html_url,
            "status": status,
            "tier": tier,
            "raw_markdown": raw,
        }


class LambdaPrizes(GithubRepoContents):
    """Stream that fetches Lambda Prize markdown files from GitHub and parses them into structured records."""

    file_prefix = "LP-"
    skip_prefix = "LP-0000"

    def parse_markdown(self, raw: str, filename: str, html_url: str) -> Optional[dict]:
        number_m = re.search(r"^(LP-\d+)", filename)
        number = number_m.group(1) if number_m else ""
        if not number or number == "LP-0000":
            return None

        h1 = re.search(r"^#\s+LP-\d+:\s*(.+)", raw, re.MULTILINE)
        title = re.sub(r"\[.*?\]\s*$", "", h1.group(1)).strip() if h1 else filename

        status_line = re.search(r"\*\*`Status[:\s]*([^`]+)`\*\*", raw, re.IGNORECASE)
        status = status_line.group(1).strip() if status_line else ""
        if not status:
            bracket = re.search(r"^#\s+LP-\d+:.*\[([^\]]+)\]", raw, re.MULTILINE)
            status = bracket.group(1).strip() if bracket else ""

        if "draft" in status.lower():
            return None

        circle_m = re.search(r"\*\*`Logos Circle[:\s]*([^`]+)`\*\*", raw, re.IGNORECASE)
        circle = circle_m.group(1).strip() if circle_m else "N/A"

        overview = ""
        ov_m = re.search(r"##\s*Overview\s*\n+([\s\S]*?)(?=\n##)", raw, re.IGNORECASE)
        if ov_m:
            lines = [
                line.replace(">", "").strip()
                for line in ov_m.group(1).split("\n")
                if line.strip() and not line.strip().startswith("TODO")
            ]
            overview = " ".join(lines)[:300]

        prize_m = re.search(r"\*\*Total Prize:?\*\*[:\s]*\$?([\w,. ]+)", raw, re.IGNORECASE)
        prize = prize_m.group(1).strip() if prize_m else "TBD"

        effort_m = re.search(r"\*\*Effort:?\*\*[:\s]*([\w/ ]+)", raw, re.IGNORECASE)
        effort = effort_m.group(1).strip() if effort_m else ""

        return {
            "number": number,
            "title": title,
            "status": status,
            "circle": circle,
            "overview": overview,
            "effort": effort,
            "prize": prize,
            "github_url": html_url,
            "raw_markdown": raw,
        }


class SourceGithubMarkdownFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            headers = {"Accept": "application/vnd.github.v3+json"}
            token = config.get("github_token")
            if token:
                headers["Authorization"] = f"token {token}"
            resp = requests.get(
                f"https://api.github.com/repos/{config['repo_rfps']}/contents/{config['rfps_path']}",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token = config.get("github_token")
        return [
            Rfps(
                repo=config["repo_rfps"],
                path=config["rfps_path"],
                github_token=token,
            ),
            LambdaPrizes(
                repo=config["repo_prizes"],
                path=config["prizes_path"],
                github_token=token,
            ),
        ]
