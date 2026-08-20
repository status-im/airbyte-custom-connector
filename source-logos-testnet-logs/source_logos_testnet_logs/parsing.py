import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

import requests

logger = logging.getLogger("airbyte")

LOG_FILE_RE = re.compile(r"logos-blockchain\.log\.(\d{4}-\d{2}-\d{2}-\d{2})")
DATE_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-\d{2}$")

PEER_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}).*/p2p/(12D3KooW\w+)")
TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
NUM_RE = re.compile(r"[0-9][0-9]+")
IP_RE = re.compile(r"/ip4/(\d+\.\d+\.\d+\.\d+)/[^/]+/\d+/[^/]+(?:/p2p/(12D3KooW\w+))?")
PRIVATE_RE = re.compile(r"^(0\.|10\.|127\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)")


def parse_index(html: str) -> List[str]:
    return sorted(set(LOG_FILE_RE.findall(html)))


def parse_log(path: Path, node: str, hour: str) -> Mapping[str, List[Mapping[str, Any]]]:
    """Parse one hourly log file into three record lists.
    Returns ``{"peers": [...], "stake": [...], "ip_peers": [...]}``.
    """
    peers: List[Mapping[str, Any]] = []
    stake_rows: List[Tuple[str, str]] = []
    ip_pairs: set = set()

    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            for ts, pid in PEER_RE.findall(line):
                peers.append({
                    "timestamp": ts,
                    "peer_id": pid,
                    "node": node,
                    "hour": hour,
                })

            if "TSI update" in line:
                row = _extract_stake_row(line)
                if row is not None:
                    stake_rows.append(row)

            for ip, pid in IP_RE.findall(line):
                if PRIVATE_RE.match(ip):
                    continue
                ip_pairs.add((ip, pid))

    stake = [
        {"timestamp": ts, "total_stake": total, "node": node, "hour": hour}
        for ts, total in stake_rows
    ]
    ip_peers = [
        {"ip": ip, "peer_id": pid or None, "node": node, "hour": hour}
        for ip, pid in ip_pairs
    ]
    return {"peers": peers, "stake": stake, "ip_peers": ip_peers}


class GeoLocator:
    """Batch-resolve public IPv4 addresses to (lat, lon, country, country_code, city)
    via the free ip-api.com batch endpoint.
    """

    BATCH_URL = (
        "http://ip-api.com/batch"
        "?fields=status,lat,lon,city,country,countryCode,query"
    )
    BATCH_SIZE = 100
    SLEEP_BETWEEN_BATCHES = 5.0

    def __init__(self, timeout: float = 30.0):
        self._cache: MutableMapping[str, Optional[Dict[str, Any]]] = {}
        self._timeout = timeout

    def enrich(self, ip_peers: List[MutableMapping[str, Any]]) -> None:
        """Mutate ip_peers in-place, adding lat/lon/country/country_code/city.

        Missing or failed lookups leave the five fields set to ``None``.
        """
        needed: List[str] = []
        seen: set = set()
        for row in ip_peers:
            ip = row.get("ip")
            if ip and ip not in self._cache and ip not in seen:
                needed.append(ip)
                seen.add(ip)

        if needed:
            self._resolve_batch(needed)

        for row in ip_peers:
            geo = self._cache.get(row.get("ip"))
            row["lat"] = geo["lat"] if geo else None
            row["lon"] = geo["lon"] if geo else None
            row["country"] = geo["country"] if geo else None
            row["country_code"] = geo["country_code"] if geo else None
            row["city"] = geo["city"] if geo else None

    def _resolve_batch(self, ips: List[str]) -> None:
        for i in range(0, len(ips), self.BATCH_SIZE):
            chunk = ips[i : i + self.BATCH_SIZE]
            try:
                resp = requests.post(self.BATCH_URL, json=chunk, timeout=self._timeout)
                resp.raise_for_status()
                payload = resp.json()
            except (requests.RequestException, ValueError) as e:
                logger.warning(f"ip-api batch lookup failed for {len(chunk)} IPs: {e}")
                for ip in chunk:
                    self._cache.setdefault(ip, None)
                continue

            returned: set = set()
            for entry in payload:
                ip = entry.get("query")
                if not ip:
                    continue
                returned.add(ip)
                if entry.get("status") == "success":
                    self._cache[ip] = {
                        "lat": entry.get("lat"),
                        "lon": entry.get("lon"),
                        "country": entry.get("country") or None,
                        "country_code": entry.get("countryCode") or None,
                        "city": entry.get("city") or None,
                    }
                else:
                    self._cache[ip] = None
            for ip in chunk:
                if ip not in returned:
                    self._cache.setdefault(ip, None)

            if i + self.BATCH_SIZE < len(ips):
                time.sleep(self.SLEEP_BETWEEN_BATCHES)


def _extract_stake_row(line: str) -> Optional[Tuple[str, str]]:
    """Return (timestamp, old_total_stake) from a ``TSI update`` line, or None."""
    ts_match = TS_RE.search(line)
    idx = line.find("old_total_stake")
    if not ts_match or idx == -1:
        return None
    rest = line[idx:]
    eq = rest.find("=")
    if eq == -1:
        return None
    num_match = NUM_RE.search(rest[eq + 1:])
    if not num_match:
        return None
    return ts_match.group(0), num_match.group(0)
