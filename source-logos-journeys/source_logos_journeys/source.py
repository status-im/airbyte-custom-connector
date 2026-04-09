import json
import logging
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .utils_github import (
    check_project,
    fetch_project_items,
    make_session,
    resolve_milestones_batch,
    resolve_refs_batch,
)
from .utils_parsing import (
    compute_action_labels,
    compute_docs_state,
    compute_red_team_state,
    compute_rnd_state,
    extract_action_labels,
    extract_blocked_teams,
    extract_doc_packet,
    extract_documentation,
    extract_journey_type,
    extract_red_team,
    extract_rnd,
    extract_target_release,
)

logger = logging.getLogger("airbyte")

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")


def _load_schema(name: str) -> dict:
    with open(os.path.join(SCHEMAS_DIR, f"{name}.json"), "r") as f:
        return json.load(f)


class _BaseStream(Stream):
    _schema_name: str = ""

    def __init__(self, source: "SourceLogosJourneys", config: Mapping[str, Any]):
        self._source = source
        self._config = config

    def get_json_schema(self) -> Mapping[str, Any]:
        return _load_schema(self._schema_name or self.name)


class JourneysStream(_BaseStream):
    primary_key = "number"
    name = "journeys"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        session = make_session(self._config["github_token"])
        owner = self._config["owner"]
        number = self._config["project_number"]
        roadmap_repo = self._config.get("roadmap_repo", "logos-co/roadmap")

        logger.info(f"Fetching project items for {owner}/project#{number}")
        items = fetch_project_items(session, owner, number)
        logger.info(f"Fetched {len(items)} items")

        # First pass: parse bodies, collect URLs to resolve
        parsed = []
        ref_urls: set = set()
        ms_urls: set = set()

        for pos, node in enumerate(items):
            c = node["content"]
            body = c.get("body") or ""
            label_names = [l["name"] for l in c.get("labels", {}).get("nodes", [])]

            rnd = extract_rnd(body)
            docs = extract_documentation(body)
            rt = extract_red_team(body)
            dp = extract_doc_packet(body)

            for u in filter(None, [docs["link"], docs["tracking"], rt["tracking"]]):
                ref_urls.add(u)
            ms_urls.update(rnd["milestones"])

            parsed.append((pos, node, c, body, label_names, rnd, dp, docs, rt))

        # Batch-resolve
        logger.info(f"Resolving {len(ref_urls)} refs, {len(ms_urls)} milestones")
        refs = resolve_refs_batch(session, list(ref_urls)) if ref_urls else {}
        milestones = resolve_milestones_batch(session, list(ms_urls), roadmap_repo) if ms_urls else {}

        # Stash caches for sibling streams
        ms_journey_map: Dict[str, List[int]] = {}
        for _, _, c, _, _, rnd, *_ in parsed:
            for u in rnd["milestones"]:
                ms_journey_map.setdefault(u, []).append(c.get("number"))
        self._source._refs_cache = refs
        self._source._milestones_cache = milestones
        self._source._milestone_journey_map = ms_journey_map

        # Second pass: compute states, yield records
        for pos, node, c, body, labels, rnd, dp, docs, rt in parsed:
            all_ms_done = bool(rnd["milestones"]) and all(
                (milestones.get(u) or {}).get("done", False) for u in rnd["milestones"]
            )
            docs_ref = refs.get(docs["link"]) if docs["link"] else None
            rt_ref = refs.get(rt["tracking"]) if rt["tracking"] else None

            rnd_st = compute_rnd_state(rnd, dp, all_ms_done)
            docs_st = compute_docs_state(docs["link"], docs_ref)
            rt_st = compute_red_team_state(rt["tracking"], rt_ref)

            assignees = [
                {"login": a["login"], "avatar_url": a.get("avatarUrl", "")}
                for a in c.get("assignees", {}).get("nodes", [])
            ]

            yield {
                "id": node["id"],
                "number": c.get("number"),
                "title": c.get("title"),
                "url": c.get("url"),
                "state": c.get("state"),
                "created_at": c.get("createdAt"),
                "updated_at": c.get("updatedAt"),
                "closed_at": c.get("closedAt"),
                "repository": c.get("repository", {}).get("nameWithOwner"),
                "position": pos,
                "body": body,
                "assignees": assignees,
                "labels": labels,
                "journey_type": extract_journey_type(labels),
                "target_release": extract_target_release(labels),
                "blocked_teams": extract_blocked_teams(labels),
                "action_labels": extract_action_labels(labels),
                "rnd_team": rnd["team"],
                "rnd_milestones": rnd["milestones"],
                "rnd_date": rnd["date"],
                "rnd_state": rnd_st,
                "doc_packet_link": dp,
                "docs_link": docs["link"],
                "docs_tracking": docs["tracking"],
                "docs_state": docs_st,
                "red_team_tracking": rt["tracking"],
                "red_team_state": rt_st,
                "computed_action_labels": compute_action_labels(rnd_st, docs_st, rt_st),
            }


class MilestonesStream(_BaseStream):
    primary_key = "url"
    name = "milestones"

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None):
        ms_cache = getattr(self._source, "_milestones_cache", None)
        journey_map = getattr(self._source, "_milestone_journey_map", None)

        if ms_cache is None:
            session = make_session(self._config["github_token"])
            items = fetch_project_items(session, self._config["owner"], self._config["project_number"])
            roadmap_repo = self._config.get("roadmap_repo", "logos-co/roadmap")
            all_urls, journey_map = set(), {}
            for node in items:
                c = node.get("content", {})
                for u in extract_rnd(c.get("body") or "")["milestones"]:
                    all_urls.add(u)
                    journey_map.setdefault(u, []).append(c.get("number"))
            ms_cache = resolve_milestones_batch(session, list(all_urls), roadmap_repo)

        for url, progress in ms_cache.items():
            yield {
                "url": url,
                "title": progress["title"] if progress else None,
                "done": progress["done"] if progress else None,
                "journey_numbers": (journey_map or {}).get(url, []),
            }


class RefsStream(_BaseStream):
    primary_key = "url"
    name = "refs"

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None):
        refs_cache = getattr(self._source, "_refs_cache", None)

        if refs_cache is None:
            session = make_session(self._config["github_token"])
            items = fetch_project_items(session, self._config["owner"], self._config["project_number"])
            all_urls = set()
            for node in items:
                body = node.get("content", {}).get("body") or ""
                docs = extract_documentation(body)
                rt = extract_red_team(body)
                for u in filter(None, [docs["link"], docs["tracking"], rt["tracking"]]):
                    all_urls.add(u)
            refs_cache = resolve_refs_batch(session, list(all_urls))

        for url, ref in refs_cache.items():
            yield {
                "url": url,
                "type": ref["type"],
                "state": ref["state"],
                "title": ref.get("title"),
                "html_url": ref.get("html_url", url),
            }



class SourceLogosJourneys(AbstractSource):

    def __init__(self):
        super().__init__()
        self._refs_cache = None
        self._milestones_cache = None
        self._milestone_journey_map = None

    def check_connection(self, logger, config):
        try:
            session = make_session(config["github_token"])
            title = check_project(session, config["owner"], config["project_number"])
            if title:
                return True, None
            return False, f"Project #{config['project_number']} not found for owner \"{config['owner']}\""
        except Exception as e:
            return False, str(e)

    def streams(self, config):
        return [
            JourneysStream(self, config),
            MilestonesStream(self, config),
            RefsStream(self, config),
        ]
