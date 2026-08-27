import base64
import logging
import time
from typing import Any, List, Mapping, Optional, Tuple

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils import AirbyteTracedException

logger = logging.getLogger("airbyte")

DEFAULT_PAGE_SIZE = 500
DEFAULT_CURSOR = "@timestamp"
DEFAULT_TIMEOUT = 60
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_MAX_RETRIES = 5
RETRY_STATUSES = (429, 502, 503, 504)


def fail(message: str, *, internal: Optional[str] = None, config_error: bool = True) -> None:
    raise AirbyteTracedException(
        message=message,
        internal_message=internal or message,
        failure_type=FailureType.config_error if config_error else FailureType.system_error,
    )


def secret_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Mapping):
        inner = value.get("password") or value.get("secret") or value.get("_secret")
        return "" if inner is None or isinstance(inner, Mapping) else str(inner)
    return str(value)


def has_auth(config: Mapping[str, Any]) -> bool:
    return bool(secret_str(config.get("username")).strip() and secret_str(config.get("password")))


def headers(config: Mapping[str, Any]) -> dict:
    out = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Connection": "keep-alive",
    }
    username = secret_str(config.get("username")).strip()
    password = secret_str(config.get("password"))
    if username and password:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        out["Authorization"] = f"Basic {token}"
    return out


def search_url(config: Mapping[str, Any]) -> str:
    return f"{config['endpoint'].rstrip('/')}/{config['index']}/_search"


def timeout(config: Mapping[str, Any]) -> Tuple[int, int]:
    """(connect, read). A short connect timeout fails fast so a retry can open a new socket."""
    read = int(config.get("request_timeout") or DEFAULT_TIMEOUT)
    connect = min(int(config.get("connect_timeout") or DEFAULT_CONNECT_TIMEOUT), read)
    return connect, read


def query_body(
    query: str,
    cursor_field: str,
    cursor_value: str,
    page_size: int,
    search_after: Optional[list],
) -> dict:
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


def post_with_retry(
    session: requests.Session,
    url: str,
    body: dict,
    request_headers: Mapping[str, str],
    config: Mapping[str, Any],
    context: str,
) -> requests.Response:
    attempts = max(0, int(config.get("max_retries") or DEFAULT_MAX_RETRIES)) + 1
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            resp = session.post(url, json=body, headers=request_headers, timeout=timeout(config))
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
        else:
            if resp.status_code not in RETRY_STATUSES:
                return resp
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
        if attempt < attempts:
            backoff = min(60, 2 ** attempt)
            logger.warning(
                f"{context}: attempt {attempt}/{attempts} failed ({last_error}); retrying in {backoff}s"
            )
            time.sleep(backoff)
    fail(
        f"{context}: giving up after {attempts} attempts. Last error: {last_error}. "
        "Check that the Airbyte worker can reach the Elasticsearch host.",
        config_error=False,
    )
