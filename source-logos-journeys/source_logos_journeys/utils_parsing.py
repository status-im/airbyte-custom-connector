import re
from typing import List, Optional

def extract_section(body: str, heading: str) -> str:
    if not body:
        return ""
    escaped = re.escape(heading)
    m = re.search(rf"^#{{1,3}}\s+{escaped}[ \t]*\r?\n", body, re.MULTILINE)
    if not m:
        return ""
    rest = body[m.end():]
    next_heading = re.search(r"^#{1,3}\s", rest, re.MULTILINE)
    return rest[:next_heading.start()] if next_heading else rest


def get_field(section: str, field: str) -> Optional[str]:
    escaped = re.escape(field)
    m = re.search(rf"^-[ \t]+{escaped}:[ \t]*(.+?)[ \t]*$", section, re.MULTILINE)
    return m.group(1).strip() if m else None


def get_field_all(section: str, field: str) -> List[str]:
    escaped = re.escape(field)
    return [
        m.group(1).strip()
        for m in re.finditer(rf"^-[ \t]+{escaped}:[ \t]*(.+?)[ \t]*$", section, re.MULTILINE)
        if m.group(1).strip()
    ]


def extract_rnd(body: str) -> dict:
    s = extract_section(body, "R&D")
    return {
        "team": get_field(s, "team"),
        "milestones": get_field_all(s, "milestone"),
        "date": get_field(s, "date"),
    }


def extract_doc_packet(body: str) -> Optional[str]:
    s = extract_section(body, "Doc Packet").strip()
    m = re.search(r"^-[ \t]+link:[ \t]*(\S+)", s, re.MULTILINE)
    return m.group(1) if m else None


def extract_documentation(body: str) -> dict:
    s = extract_section(body, "Documentation")
    link_m = re.search(r"^-[ \t]+link:[ \t]*(\S+)", s, re.MULTILINE)
    tracking_m = re.search(r"^-[ \t]+tracking:[ \t]*(\S+)", s, re.MULTILINE)
    if link_m:
        link = link_m.group(1)
    else:
        s_no_tracking = re.sub(r"^-[ \t]+tracking:.*$", "", s, flags=re.MULTILINE)
        url_m = re.search(r"https?://\S+", s_no_tracking)
        link = re.sub(r"[)\].,;>]+$", "", url_m.group(0)) if url_m else None
    return {"link": link, "tracking": tracking_m.group(1) if tracking_m else None}


def extract_red_team(body: str) -> dict:
    s = extract_section(body, "Red Team")
    m = re.search(r"^-[ \t]+tracking:[ \t]*(\S+)", s, re.MULTILINE)
    return {"tracking": m.group(1) if m else None}


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------

def extract_journey_type(labels: List[str]) -> Optional[str]:
    for name in labels:
        if re.match(r"^(gui user|developer|node operator)$", name.strip(), re.IGNORECASE):
            return name.strip().lower()
    return None


def extract_target_release(labels: List[str]) -> Optional[str]:
    for name in labels:
        if re.match(r"^testnet\b", name.strip(), re.IGNORECASE):
            return name.strip()
    return None


def extract_blocked_teams(labels: List[str]) -> List[str]:
    seen = set()
    out = []
    for name in labels:
        m = re.match(r"^blocked-by:(.+)$", name, re.IGNORECASE)
        if not m:
            continue
        team = m.group(1).strip()
        if team and team not in seen:
            seen.add(team)
            out.append(team)
    return out


def extract_status(labels: List[str]) -> Optional[str]:
    for name in labels:
        m = re.match(r"^status:(.+)$", name, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None
