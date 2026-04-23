import logging
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

from .parsing import GeoLocator, parse_index, parse_log

logger = logging.getLogger("airbyte")


class LogProcessor:
    """Downloads hourly log files once per (node, hour), parses them into three
    record lists, deletes the local file, and caches results in memory so the
    three streams can share a single fetch+parse pass within a sync."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        tmp_dir: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, password)
        self.tmp_dir = Path(tmp_dir or tempfile.mkdtemp(prefix="logos-logs-"))
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self._cache: dict = {}
        self._hours_cache: dict = {}
        self._geolocator = GeoLocator()

    def list_hours(self, node: str) -> List[str]: #fetch the houtly log files that exist for a given node
        if node in self._hours_cache:
            return self._hours_cache[node]
        url = f"{self.base_url}/{node}/"
        logger.info(f"Fetching index {url}")
        resp = requests.get(url, auth=self.auth, timeout=60)
        resp.raise_for_status()
        hours = parse_index(resp.text)
        self._hours_cache[node] = hours
        return hours

    def get_parsed(self, node: str, hour: str) -> Mapping[str, List[Mapping[str, Any]]]: # gives us which streams were already parsed for this hour and node, and gives us the parsed data.
        key = (node, hour)
        if key in self._cache:
            return self._cache[key]
        path = self._download(node, hour)
        try:
            parsed = parse_log(path, node, hour)
        finally:
            try:
                path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Failed to delete {path}: {e}")
        self._geolocator.enrich(parsed["ip_peers"])
        self._cache[key] = parsed
        return parsed

    def _download(self, node: str, hour: str) -> Path: #fetches one hourly log file ans returns the local path
        filename = f"logos-blockchain.log.{hour}"
        out = self.tmp_dir / node / filename
        out.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self.base_url}/{node}/{filename}"
        if out.exists():
            headers = {"Range": f"bytes={out.stat().st_size}-"}
            mode = "ab"
        else:
            headers = {}
            mode = "wb"
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(url, auth=self.auth, headers=headers, stream=True, timeout=120)
                if r.status_code == 416:
                    return out
                r.raise_for_status()
                with open(out, mode) as f:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        if chunk:
                            f.write(chunk)
                return out
            except requests.RequestException as e:
                last_err = e
                logger.warning(f"Download attempt {attempt + 1} for {url} failed: {e}")
                time.sleep(2)
        assert last_err is not None
        raise last_err


class _BaseLogStream(Stream, IncrementalMixin):
    """Shared skeleton for the three log-derived streams"""

    cursor_field = "hour"
    _record_key: str = ""

    def __init__(self, processor: LogProcessor, nodes: List[str], start_date: str, end_date: Optional[str] = None):
        super().__init__()
        self.processor = processor
        self.nodes = nodes
        self.start_date = start_date
        self.end_date = end_date
        self._state: MutableMapping[str, Any] = {"per_node": {}}

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = {"per_node": dict((value or {}).get("per_node", {}))}

    def _initial_floor(self, node: str, all_hours: List[str]) -> str:
        # start_date is "YYYY-MM-DD-HH". The slice filter uses `hour > floor`,
        # so to make start_date itself *inclusive* we step one hour back and
        # use that as the floor.
        dt = datetime.strptime(self.start_date, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)
        return (dt - timedelta(hours=1)).strftime("%Y-%m-%d-%H")

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        state_per_node = (stream_state or self._state or {}).get("per_node", {})
        ceiling = self.end_date  # already "YYYY-MM-DD-HH" or None
        for node in self.nodes:
            hours = self.processor.list_hours(node)
            floor = state_per_node.get(node)
            if not floor:
                floor = self._initial_floor(node, hours)
            for hour in hours:
                if hour > floor and (ceiling is None or hour <= ceiling):
                    yield {"node": node, "hour": hour}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        if not stream_slice:
            return
        node = stream_slice["node"]
        hour = stream_slice["hour"]
        parsed = self.processor.get_parsed(node, hour)
        for record in parsed[self._record_key]:
            yield record
        prev = self._state.get("per_node", {}).get(node, "")
        if hour > prev:
            self._state.setdefault("per_node", {})[node] = hour


class PeersStream(_BaseLogStream):
    _record_key = "peers"
    primary_key = ["timestamp", "peer_id", "node"]

    @property
    def name(self) -> str:
        return "peers"


class StakeStream(_BaseLogStream):
    _record_key = "stake"
    primary_key = ["timestamp", "node"]

    @property
    def name(self) -> str:
        return "stake"


class IpPeersStream(_BaseLogStream):
    _record_key = "ip_peers"
    primary_key = ["ip", "peer_id", "node"]

    @property
    def name(self) -> str:
        return "ip_peers"


class SourceLogosTestnetLogs(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            base_url = config.get("base_url", "https://testnet.blockchain.logos.co/internal/node-data").rstrip("/")
            nodes = config.get("nodes") or []
            if not nodes:
                return False, "At least one node must be configured."
            auth = HTTPBasicAuth(config.get("username", "strode"), config["password"])
            for node in nodes:
                url = f"{base_url}/{node}/"
                r = requests.get(url, auth=auth, timeout=30)
                if r.status_code != 200:
                    return False, f"GET {url} returned HTTP {r.status_code}"
            return True, None
        except Exception as e:
            return False, f"Connection check failed: {e}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        base_url = config.get("base_url", "https://testnet.blockchain.logos.co/internal/node-data")
        nodes = [str(n) for n in (config.get("nodes") or ["0", "1", "2", "3"])]
        now_utc = datetime.now(timezone.utc)
        start_date = config.get("start_date") or (
            now_utc - timedelta(hours=2)
        ).strftime("%Y-%m-%d-%H")
        # Default ceiling = last complete UTC hour at sync start. Pinning this
        # here (once per sync) guarantees all nodes and all three streams stop
        # at the same hour even though `list_hours` is re-fetched per node /
        # per stream and an hour may roll over mid-sync.
        end_date = config.get("end_date") or (
            now_utc - timedelta(hours=1)
        ).strftime("%Y-%m-%d-%H")
        processor = LogProcessor(
            base_url=base_url,
            username=config.get("username", "strode"),
            password=config["password"],
        )
        return [
            PeersStream(processor, nodes, start_date, end_date),
            StakeStream(processor, nodes, start_date, end_date),
            IpPeersStream(processor, nodes, start_date, end_date),
        ]
