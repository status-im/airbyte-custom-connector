import base64
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils import AirbyteTracedException

logger = logging.getLogger("airbyte")

DEFAULT_PAGE_SIZE = 5000
MAX_PAGE_SIZE = 10000
DEFAULT_CURSOR = "@timestamp"
DEFAULT_TIMEOUT = 120
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_MAX_RETRIES = 5
DEFAULT_KEEP_ALIVE = "5m"
DEFAULT_LOOKBACK_HOURS = 1
DEFAULT_INGESTION_LAG_SECONDS = 60
DEFAULT_SUBDIVIDE_THRESHOLD = 200000
DEFAULT_NEIGHBOUR_DAYS = 1

RETRY_STATUSES = (408, 429)
CONFIG_ERROR_STATUSES = (401, 403)
PIT_UNSUPPORTED_STATUSES = (400, 404, 405, 501)

LENIENT_INDEX_PARAMS = {"ignore_unavailable": "true", "allow_no_indices": "true"}

TIMESTAMP_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"[Tt ](?P<hour>\d{2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.(?P<fraction>\d+))?)?"
    r"(?P<offset>[Zz]|[+-]\d{2}:?\d{2})?$"
)


class TransientElasticsearchError(Exception):
    """Retryable: a connection problem, a 5xx, or a 200 whose payload is incomplete."""


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


def base_url(config: Mapping[str, Any]) -> str:
    return config["endpoint"].rstrip("/")


def timeout(config: Mapping[str, Any]) -> Tuple[int, int]:
    """(connect, read). A short connect timeout fails fast so a retry can open a new socket."""
    read = int(config.get("request_timeout") or DEFAULT_TIMEOUT)
    connect = min(int(config.get("connect_timeout") or DEFAULT_CONNECT_TIMEOUT), read)
    return connect, read


def page_size(config: Mapping[str, Any]) -> int:
    size = int(config.get("page_size") or DEFAULT_PAGE_SIZE)
    if size < 1:
        size = DEFAULT_PAGE_SIZE
    if size > MAX_PAGE_SIZE:
        logger.warning(
            f"page_size {size} exceeds index.max_result_window ({MAX_PAGE_SIZE}); using {MAX_PAGE_SIZE}."
        )
        size = MAX_PAGE_SIZE
    return size


def keep_alive(config: Mapping[str, Any]) -> str:
    return str(config.get("keep_alive") or DEFAULT_KEEP_ALIVE)


def parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, bool):
        fail(f"Cannot read a timestamp from {value!r}.", config_error=False)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)

    text = str(value).strip()
    if not text:
        fail("Cannot read a timestamp from an empty value.", config_error=False)
    if re.fullmatch(r"-?\d+", text):
        return datetime.fromtimestamp(int(text) / 1000.0, tz=timezone.utc)

    match = TIMESTAMP_RE.match(text)
    if not match:
        fail(
            f"Could not parse the timestamp {text!r}. Expected ISO-8601, "
            "for example 2026-08-05T00:00:00Z.",
            config_error=False,
        )
    parts = match.groupdict()
    fraction = (parts["fraction"] or "")[:6].ljust(6, "0")
    parsed = datetime(
        int(parts["year"]),
        int(parts["month"]),
        int(parts["day"]),
        int(parts["hour"]),
        int(parts["minute"]),
        int(parts["second"] or 0),
        int(fraction),
        tzinfo=timezone.utc,
    )
    offset = parts["offset"]
    if offset and offset not in ("Z", "z"):
        sign = 1 if offset[0] == "+" else -1
        digits = offset[1:].replace(":", "")
        parsed -= sign * timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
    return parsed


def iso_z(value: Any) -> str:
    """Render an instant as millisecond-precision UTC, the form Elasticsearch round-trips."""
    moment = parse_timestamp(value) if not isinstance(value, datetime) else value
    moment = moment.astimezone(timezone.utc) if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{moment.microsecond // 1000:03d}Z"


def phrase_filters(items: Optional[Sequence[Mapping[str, Any]]], context: str = "filters") -> List[dict]:
    """Build ``match_phrase`` clauses. Used instead of ``keyword`` terms because every
    string field on these indices is ``text`` with no usable ``keyword`` subfield."""
    clauses: List[dict] = []
    for item in items or []:
        if not isinstance(item, Mapping):
            fail(f"Each entry in {context} must be an object with 'field' and 'phrase'.")
        field = str(item.get("field") or "").strip()
        phrase = item.get("phrase")
        if not field or phrase is None or str(phrase).strip() == "":
            fail(f"Each entry in {context} needs a non-empty 'field' and 'phrase'.")
        clauses.append({"match_phrase": {field: str(phrase)}})
    return clauses


def build_query(
    cursor_field: str,
    gte: str,
    lt: str,
    *,
    query_string: Optional[str] = None,
    filters: Optional[Sequence[dict]] = None,
) -> dict:
    """A filter-only bool query for a half-open ``[gte, lt)`` slice of the cursor field."""
    clauses: List[dict] = [
        {"range": {cursor_field: {"gte": gte, "lt": lt, "format": "strict_date_optional_time"}}}
    ]
    clauses.extend(filters or [])
    if query_string and query_string.strip():
        clauses.append({"query_string": {"query": query_string.strip()}})
    return {"bool": {"filter": clauses}}


def assert_response_complete(payload: Mapping[str, Any], context: str) -> None:
    if payload.get("timed_out"):
        raise TransientElasticsearchError(
            f"{context}: Elasticsearch returned timed_out=true, so this page is partial"
        )
    shards = payload.get("_shards") or {}
    total = int(shards.get("total") or 0)
    successful = int(shards.get("successful") or 0)
    skipped = int(shards.get("skipped") or 0)
    failed = int(shards.get("failed") or 0)
    if failed or (total and successful + skipped < total):
        detail = json.dumps(shards.get("failures") or [])[:800]
        raise TransientElasticsearchError(
            f"{context}: {failed} of {total} shards failed "
            f"(successful={successful}, skipped={skipped}), so this page is partial. {detail}"
        )


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    config: Mapping[str, Any],
    context: str,
    *,
    body: Optional[dict] = None,
    params: Optional[Mapping[str, str]] = None,
    allow_statuses: Sequence[int] = (),
    validate: bool = True,
) -> Tuple[int, Optional[dict]]:
    attempts = max(0, int(config.get("max_retries") or DEFAULT_MAX_RETRIES)) + 1
    request_headers = headers(config)
    read_timeout = timeout(config)
    last_error = ""

    for attempt in range(1, attempts + 1):
        try:
            resp = session.request(
                method, url, json=body, params=params, headers=request_headers, timeout=read_timeout
            )
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
        else:
            status = resp.status_code
            if status in allow_statuses:
                return status, None
            if status in CONFIG_ERROR_STATUSES:
                fail(
                    f"{context}: Elasticsearch HTTP {status} for {url}: {resp.text[:500]}",
                    config_error=True,
                )
            if status >= 500 or status in RETRY_STATUSES:
                # Circuit breaker trips arrive as 429 and search-phase failures as 5xx.
                last_error = f"HTTP {status}: {resp.text[:300]}"
            elif status >= 400:
                fail(
                    f"{context}: Elasticsearch HTTP {status} for {url}: {resp.text[:500]}",
                    config_error=False,
                )
            else:
                try:
                    payload = resp.json()
                except ValueError:
                    last_error = f"HTTP {status} with a non-JSON body: {resp.text[:200]}"
                else:
                    if not validate:
                        return status, payload
                    try:
                        assert_response_complete(payload, context)
                    except TransientElasticsearchError as e:
                        last_error = str(e)
                    else:
                        return status, payload

        if attempt < attempts:
            backoff = min(60, 2 ** attempt)
            logger.warning(
                f"{context}: attempt {attempt}/{attempts} failed ({last_error}); retrying in {backoff}s"
            )
            time.sleep(backoff)

    fail(
        f"{context}: giving up after {attempts} attempts. Last error: {last_error}. "

        config_error=False,
    )


def count_documents(
    session: requests.Session,
    config: Mapping[str, Any],
    indices: str,
    query: dict,
    context: str,
) -> int:
    """``_count`` for the exact slice query. The completeness assertion compares against this."""
    url = f"{base_url(config)}/{indices}/_count"
    _, payload = request_with_retry(
        session, "POST", url, config, context, body={"query": query}, params=LENIENT_INDEX_PARAMS
    )
    return int((payload or {}).get("count") or 0)


def open_pit(
    session: requests.Session,
    config: Mapping[str, Any],
    indices: str,
    context: str,
) -> Optional[str]:
    """Open a point-in-time context. Returns ``None`` when the cluster has no PIT API."""
    url = f"{base_url(config)}/{indices}/_pit"
    params = dict(LENIENT_INDEX_PARAMS, keep_alive=keep_alive(config))
    status, payload = request_with_retry(
        session, "POST", url, config, context, params=params, allow_statuses=PIT_UNSUPPORTED_STATUSES
    )
    if payload is None:
        logger.warning(
            f"{context}: the point-in-time API is unavailable (HTTP {status}); using the scroll API."
        )
        return None
    pit_id = payload.get("id")
    if not pit_id:
        fail(f"{context}: _pit returned no id: {json.dumps(payload)[:300]}", config_error=False)
    return pit_id


def close_pit(session: requests.Session, config: Mapping[str, Any], pit_id: str, context: str) -> None:
    url = f"{base_url(config)}/_pit"
    try:
        request_with_retry(
            session,
            "DELETE",
            url,
            config,
            f"{context}: closing the point-in-time context",
            body={"id": pit_id},
            allow_statuses=PIT_UNSUPPORTED_STATUSES,
            validate=False,
        )
    except Exception as e:
        logger.warning(f"{context}: could not close the point-in-time context: {e}")


def open_scroll(
    session: requests.Session,
    config: Mapping[str, Any],
    indices: str,
    body: dict,
    context: str,
) -> dict:
    url = f"{base_url(config)}/{indices}/_search"
    params = dict(LENIENT_INDEX_PARAMS, scroll=keep_alive(config))
    _, payload = request_with_retry(session, "POST", url, config, context, body=body, params=params)
    return payload or {}


def next_scroll(
    session: requests.Session,
    config: Mapping[str, Any],
    scroll_id: str,
    context: str,
) -> dict:
    url = f"{base_url(config)}/_search/scroll"
    _, payload = request_with_retry(
        session,
        "POST",
        url,
        config,
        context,
        body={"scroll": keep_alive(config), "scroll_id": scroll_id},
    )
    return payload or {}


def close_scroll(
    session: requests.Session, config: Mapping[str, Any], scroll_id: str, context: str
) -> None:
    url = f"{base_url(config)}/_search/scroll"
    try:
        request_with_retry(
            session,
            "DELETE",
            url,
            config,
            f"{context}: closing the scroll context",
            body={"scroll_id": [scroll_id]},
            allow_statuses=PIT_UNSUPPORTED_STATUSES,
            validate=False,
        )
    except Exception as e:
        logger.warning(f"{context}: could not close the scroll context: {e}")


def search_body(
    cursor_field: str,
    query: dict,
    size: int,
    *,
    pit: Optional[Mapping[str, str]] = None,
    search_after: Optional[list] = None,
    source_fields: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """A ``search_after`` page.

    ``_shard_doc`` is mandatory as the tiebreaker: ``@timestamp`` collides at millisecond
    resolution and sorting on it alone drops tied documents at every page boundary.
    """
    body: Dict[str, Any] = {
        "size": size,
        "track_total_hits": False,
        "sort": [{cursor_field: "asc"}, {"_shard_doc": "asc"}],
        "query": query,
    }
    if source_fields:
        body["_source"] = list(source_fields)
    if pit:
        body["pit"] = dict(pit)
    if search_after:
        body["search_after"] = list(search_after)
    return body


def scroll_body(
    query: dict,
    size: int,
    *,
    source_fields: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """A scroll page. ``_doc`` order is the cheapest total order and is stable within a scroll."""
    body: Dict[str, Any] = {
        "size": size,
        "track_total_hits": False,
        "sort": ["_doc"],
        "query": query,
    }
    if source_fields:
        body["_source"] = list(source_fields)
    return body
