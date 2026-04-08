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

    file_prefix: str = ""
    skip_prefix: str = ""

    def __init__(self, repo: str, path: str, github_token: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.repo = repo
        self.repo_path = path
        self.github_token = github_token

    def _auth_headers(self) -> dict:
        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.github_token:
            headers["Authorization"] = f"token {self.github_token}"
        return headers

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return self._auth_headers()

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, **kwargs) -> str:
        return f"repos/{self.repo}/contents/{self.repo_path}"

    def _get_file_history(self, filename: str) -> Mapping[str, Optional[str]]:
        url = f"https://api.github.com/repos/{self.repo}/commits"
        params = {"path": f"{self.repo_path}/{filename}", "per_page": 100}
        resp = requests.get(url, headers=self._auth_headers(), params=params, timeout=30)
        if resp.status_code != 200 or not resp.json():
            return {"created_at": None, "last_modified_at": None, "created_by": None}
        commits = resp.json()
        first_commit = commits[-1]
        author = first_commit.get("author") or {}
        return {
            "created_at": first_commit["commit"]["committer"]["date"],
            "last_modified_at": commits[0]["commit"]["committer"]["date"],
            "created_by": author.get("login"),
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        files = response.json()
        if not isinstance(files, list):
            return

        md_files = [
            f for f in files
            if f["name"].endswith(".md")
            and f["name"].startswith(self.file_prefix)
            and not f["name"].startswith(self.skip_prefix)
        ]

        for f in md_files:
            raw_resp = requests.get(f["download_url"], headers=self._auth_headers(), timeout=30)
            raw_resp.raise_for_status()
            parsed = self.parse_markdown(raw_resp.text, f["name"], f["html_url"])
            if parsed:
                parsed.update(self._get_file_history(f["name"]))
                yield parsed

    def _extract_number(self, filename: str) -> Optional[str]:
        m = re.search(rf"^({re.escape(self.file_prefix)}\d+)", filename)
        number = m.group(1) if m else ""
        if not number or number == self.skip_prefix:
            return None
        return number

    def _extract_field(self, raw: str, *patterns: str, default: str = "") -> str:
        for pat in patterns:
            m = re.search(pat, raw, re.IGNORECASE)
            if m:
                return re.sub(r"[`*]", "", m.group(1)).strip()
        return default

    def _extract_overview(self, raw: str, max_len: int = 200, strip_chars: str = "") -> str:
        ov_m = re.search(r"##\s*(?:\U0001f9ed\s*)?Overview\s*\n+([\s\S]*?)(?=\n##)", raw, re.IGNORECASE)
        if ov_m:
            lines = []
            for line in ov_m.group(1).split("\n"):
                cleaned = line.strip()
                if not cleaned or cleaned.startswith("TODO"):
                    continue
                for ch in strip_chars:
                    cleaned = cleaned.replace(ch, "")
                lines.append(cleaned.strip())
            if lines:
                return " ".join(lines)[:max_len]

        lines = [
            line.strip() for line in raw.split("\n")
            if line.strip()
            and not line.startswith(("#", "|", "---", "**"))
        ]
        return " ".join(lines[:3])[:max_len]

    def parse_markdown(self, raw: str, filename: str, html_url: str) -> Optional[dict]:
        raise NotImplementedError


class Rfps(GithubRepoContents):
    """Stream that fetches RFP markdown files from GitHub and parses them into structured records."""

    file_prefix = "RFP-"
    skip_prefix = "RFP-000"

    def parse_markdown(self, raw: str, filename: str, html_url: str) -> Optional[dict]:
        number = self._extract_number(filename)
        if not number:
            return None

        h1 = re.search(r"^#\s+(.+)", raw, re.MULTILINE)
        title = re.sub(r"^RFP-\d+[:\s\-\u2014]+", "", h1.group(1)).strip() if h1 else filename

        status = self._extract_field(
            raw,
            r"\*\*Status\*\*[:\s]*(.+)",
            r"Status[:\s|]*([^\n|]+)",
            default="open",
        )
        if "draft" in status.lower():
            return None

        return {
            "number": number,
            "title": title,
            "status": status,
            "category": self._extract_field(raw, r"\*\*Category\*\*[:\s]*(.+)", r"Category[:\s|]*([^\n|]+)"),
            "tier": self._extract_field(raw, r"\*\*Tier\*\*[:\s]*(.+)", r"Tier[:\s|]*([^\n|]+)"),
            "summary": self._extract_overview(raw, max_len=200),
            "github_url": html_url,
            "raw_markdown": raw,
        }


class LambdaPrizes(GithubRepoContents):
    """Stream that fetches Lambda Prize markdown files from GitHub and parses them into structured records."""

    file_prefix = "LP-"
    skip_prefix = "LP-0000"

    def parse_markdown(self, raw: str, filename: str, html_url: str) -> Optional[dict]:
        number = self._extract_number(filename)
        if not number:
            return None

        h1 = re.search(r"^#\s+LP-\d+:\s*(.+)", raw, re.MULTILINE)
        title = re.sub(r"\[.*?\]\s*$", "", h1.group(1)).strip() if h1 else filename

        status = self._extract_field(
            raw,
            r"\*\*`Status[:\s]*([^`]+)`\*\*",
            r"^#\s+LP-\d+:.*\[([^\]]+)\]",
        )
        if "draft" in status.lower():
            return None

        return {
            "number": number,
            "title": title,
            "status": status,
            "circle": self._extract_field(raw, r"\*\*`Logos Circle[:\s]*([^`]+)`\*\*", default="N/A"),
            "overview": self._extract_overview(raw, max_len=300, strip_chars=">"),
            "effort": self._extract_field(raw, r"\*\*Effort:?\*\*[:\s]*([\w/ ]+)"),
            "prize": self._extract_field(raw, r"\*\*Total Prize:?\*\*[:\s]*\$?([\w,. ]+)", default="TBD"),
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
