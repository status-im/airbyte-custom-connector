import base64
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests

GRAPHQL_URL = "https://api.github.com/graphql"
REST_BASE = "https://api.github.com"

_PROJECT_ITEMS_QUERY_TPL = """
query($owner: String!, $number: Int!, $cursor: String) {{
  {root_field}(login: $owner) {{
    projectV2(number: $number) {{
      id
      title
      items(first: 50, after: $cursor) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{
          id
          content {{
            ... on Issue {{
              number title body state createdAt updatedAt closedAt url
              repository {{ nameWithOwner }}
              labels(first: 20) {{ nodes {{ name color }} }}
              assignees(first: 5) {{ nodes {{ login avatarUrl }} }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

_CHECK_QUERY_TPL = """
query($owner: String!, $number: Int!) {{
  {root_field}(login: $owner) {{
    projectV2(number: $number) {{ title }}
  }}
}}
"""


def make_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    return session


def graphql(session: requests.Session, query: str, variables: dict) -> dict:
    resp = session.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables},
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        msgs = "; ".join(e["message"] for e in data["errors"])
        raise RuntimeError(f"GraphQL error: {msgs}")
    return data["data"]


def _fetch_page(session, owner, number, cursor, use_org):
    root = "organization" if use_org else "user"
    query = _PROJECT_ITEMS_QUERY_TPL.format(root_field=root)
    try:
        data = graphql(session, query, {"owner": owner, "number": number, "cursor": cursor})
    except RuntimeError:
        if not use_org:
            return None
        raise
    node = data.get(root)
    if not node or not node.get("projectV2"):
        return None
    project = node["projectV2"]
    page = project["items"]
    return page["nodes"], page["pageInfo"]["hasNextPage"], page["pageInfo"]["endCursor"]


def fetch_project_items(session: requests.Session, owner: str, number: int) -> List[dict]:
    """Fetch all project items in board order, with user -> org fallback."""
    result = _fetch_page(session, owner, number, None, False)
    use_org = False
    if result is None:
        result = _fetch_page(session, owner, number, None, True)
        if result is None:
            raise RuntimeError(f'Could not find project #{number} for owner "{owner}".')
        use_org = True

    nodes, has_next, cursor = result
    all_nodes = list(nodes)

    while has_next:
        result = _fetch_page(session, owner, number, cursor, use_org)
        if not result:
            break
        nodes, has_next, cursor = result
        all_nodes.extend(nodes)

    return [n for n in all_nodes if n.get("content") and n["content"].get("title")]


def check_project(session: requests.Session, owner: str, number: int) -> Optional[str]:
    """Return project title if reachable, else None."""
    for root in ("user", "organization"):
        query = _CHECK_QUERY_TPL.format(root_field=root)
        try:
            data = graphql(session, query, {"owner": owner, "number": number})
            node = data.get(root)
            if node and node.get("projectV2"):
                return node["projectV2"]["title"]
        except RuntimeError:
            continue
    return None


# ---------------------------------------------------------------------------
# Ref resolution
# ---------------------------------------------------------------------------

_PR_RE = re.compile(r"github\.com/([\w.-]+)/([\w.-]+)/pull/(\d+)")
_ISSUE_RE = re.compile(r"github\.com/([\w.-]+)/([\w.-]+)/issues/(\d+)")


def resolve_ref(session: requests.Session, url: str) -> dict:
    pr_m = _PR_RE.search(url)
    issue_m = _ISSUE_RE.search(url)
    if not pr_m and not issue_m:
        return {"type": "url", "state": "merged", "title": None, "html_url": url}
    try:
        if pr_m:
            o, r, n = pr_m.group(1), pr_m.group(2), pr_m.group(3)
            pr = session.get(f"{REST_BASE}/repos/{o}/{r}/pulls/{n}").json()
            state = "merged" if pr.get("merged_at") else pr["state"]
            return {"type": "pr", "state": state, "title": pr["title"], "html_url": pr["html_url"]}
        o, r, n = issue_m.group(1), issue_m.group(2), issue_m.group(3)
        issue = session.get(f"{REST_BASE}/repos/{o}/{r}/issues/{n}").json()
        return {"type": "issue", "state": issue["state"], "title": issue["title"], "html_url": issue["html_url"]}
    except Exception:
        return {"type": "pr" if pr_m else "issue", "state": "error", "title": None, "html_url": url}


def resolve_refs_batch(session: requests.Session, urls: List[str]) -> Dict[str, dict]:
    results = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(resolve_ref, session, u): u for u in urls}
        for f in as_completed(futures):
            url = futures[f]
            try:
                results[url] = f.result()
            except Exception:
                results[url] = {"type": "error", "state": "error", "title": None, "html_url": url}
    return results


# ---------------------------------------------------------------------------
# Milestone progress
# ---------------------------------------------------------------------------

def resolve_milestones_batch(
    session: requests.Session,
    urls: List[str],
    roadmap_repo: str,
) -> Dict[str, Optional[dict]]:
    parent_cache: Dict[str, Optional[str]] = {}
    return {url: _fetch_milestone(session, url, roadmap_repo, parent_cache) for url in urls}


def _fetch_milestone(session, url, roadmap_repo, parent_cache):
    if not url or not url.startswith("https://roadmap.logos.co/"):
        return None

    path = url.replace("https://roadmap.logos.co/", "").rstrip("/")
    parts = path.split("/")
    slug = parts.pop()
    parent_path = f"content/{'/'.join(parts)}/index.md"

    if parent_path not in parent_cache:
        try:
            resp = session.get(f"{REST_BASE}/repos/{roadmap_repo}/contents/{parent_path}")
            resp.raise_for_status()
            parent_cache[parent_path] = base64.b64decode(resp.json()["content"]).decode("utf-8")
        except Exception:
            parent_cache[parent_path] = None

    content = parent_cache[parent_path]
    if not content:
        return None

    m = re.search(
        rf"^\s*- \[(x| )\]\s*\[([^\]]+)\]\(\./{re.escape(slug)}\.md\)",
        content, re.MULTILINE,
    )
    return {"title": m.group(2).strip(), "done": m.group(1) == "x"} if m else None
