"""AoE2:DE player search + match history, sourced from the official Relic /
World's Edge community API -- the same backend aoe2insights.com,
aoe2companion.com and aoestats.io are themselves built on top of.

This replaces an earlier version of this module that scraped aoe2insights.com's
HTML directly. That approach hit a hard WAF block (HTTP 403 on every request,
independent of headers/session/User-Agent) that's specific to that site's own
bot protection -- it wasn't a restriction on the underlying data. Talking to
the Relic API directly is both more reliable and the more honest approach: it's
the same public, unauthenticated JSON API every third-party AoE2 stats site
already depends on, not a workaround of anything.

Two real limitations of this API, by design, not oversight:
  - Player lookup (`GetPersonalStat` with `aliases=[...]`) is an EXACT alias
    match, not fuzzy/typeahead search -- there is no partial-name search
    endpoint on this backend. `search_player()` tries a small number of case
    variants of what was typed to raise the odds of a hit, then gives up.
  - There's no "opening strategy" field anywhere in this API. `strategy` is
    always None here; that's a real gap, not a bug -- the UI already handles
    an empty strategy breakdown gracefully.
"""
import json
import logging
import time
from datetime import datetime, timezone

import requests

import aoe2ref

LOGGER = logging.getLogger("scraper")

BASE = "https://aoe-api.worldsedgelink.com"
HEADERS = {
    "User-Agent": "AoE2CampaignReport/1.0 (+https://github.com/vexer23/aoe2)",
}
TIMEOUT = 20
TITLE = "age2"

# leaderboard_id -> short label, for turning a raw rating into something
# readable like "RM 1v1 1850". Values corroborated across multiple
# actively-maintained community tools that consume this same API.
LEADERBOARD_LABELS = {
    3: "RM 1v1",
    4: "RM Team",
    13: "EW 1v1",
    14: "EW Team",
    17: "EW 1v1",
}


def _get(path, params):
    url = f"{BASE}{path}"
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    except requests.RequestException as e:
        LOGGER.warning("request to %s failed: %s", url, e)
        raise
    if resp.status_code >= 400:
        LOGGER.warning("request to %s returned %s; body starts with: %r",
                        resp.url, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    try:
        return resp.json()
    except ValueError:
        LOGGER.warning("non-JSON response from %s; body starts with: %r", resp.url, resp.text[:300])
        raise


def _best_rating_label(statgroup_id, leaderboard_stats):
    """Pick one representative rating to show for a profile: prefer RM 1v1,
    then RM Team, then whatever else is present."""
    by_leaderboard = {
        s["leaderboard_id"]: s for s in leaderboard_stats
        if s.get("statgroup_id") == statgroup_id
    }
    for lb_id in (3, 4, 13, 14, 17):
        s = by_leaderboard.get(lb_id)
        if s and s.get("rating") is not None:
            return f"{LEADERBOARD_LABELS.get(lb_id, 'RM')} {s['rating']}"
    for s in by_leaderboard.values():
        if s.get("rating") is not None:
            return f"{LEADERBOARD_LABELS.get(s.get('leaderboard_id'), 'Rated')} {s['rating']}"
    return None


def _personal_stat(alias):
    """One GetPersonalStat call for a given alias string. Returns the raw
    parsed JSON dict, or None on failure."""
    try:
        data = _get("/community/leaderboard/GetPersonalStat", {
            "title": TITLE,
            "aliases": json.dumps([alias]),
        })
    except (requests.RequestException, ValueError):
        return None
    if data.get("statGroups"):
        return data
    return None


def search_player(name, max_candidates=8):
    """Look up a player by exact display name/alias (case variants are tried
    since the API does exact matching). Returns candidate dicts:
    {"name", "profile_id", "country", "rating_label"}."""
    name = (name or "").strip()
    if not name:
        return []

    tried = []
    for variant in {name, name.lower(), name.upper(), name.title(), name.capitalize()}:
        if variant in tried:
            continue
        tried.append(variant)
        data = _personal_stat(variant)
        if data:
            break
    else:
        LOGGER.info("search_debug no candidates found for %r after trying case variants %r", name, tried)
        return []

    leaderboard_stats = data.get("leaderboardStats") or []
    candidates = []
    seen_ids = set()
    for group in data.get("statGroups") or []:
        for member in group.get("members") or []:
            profile_id = member.get("profile_id")
            if profile_id is None or profile_id in seen_ids:
                continue
            seen_ids.add(profile_id)
            candidates.append({
                "name": member.get("alias") or name,
                "profile_id": str(profile_id),
                "country": (member.get("country") or "").upper() or None,
                "rating_label": _best_rating_label(group.get("id"), leaderboard_stats),
            })
            if len(candidates) >= max_candidates:
                return candidates
    return candidates


def _prettify_map(slug):
    if not slug:
        return None
    base = slug.split(".")[0].replace("_", " ").replace("-", " ").strip()
    if not base:
        return None
    pretty = base.title()
    # Prefer the canonical display name from the reference dataset when the
    # prettified slug matches one, case-insensitively (fixes things like
    # capitalization of "of" / "the" that .title() gets wrong).
    for known in aoe2ref.MAP_NAMES:
        if known.lower() == pretty.lower():
            return known
    return pretty


def _time_ago(unix_seconds):
    if not unix_seconds:
        return None
    try:
        then = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None
    delta = datetime.now(timezone.utc) - then
    mins = int(delta.total_seconds() // 60)
    if mins < 1:
        return "just now"
    if mins < 60:
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = hours // 24
    if days < 30:
        return f"{days} day{'s' if days != 1 else ''} ago"
    months = days // 30
    if months < 12:
        return f"{months} month{'s' if months != 1 else ''} ago"
    years = days // 365
    return f"{years} year{'s' if years != 1 else ''} ago"


def get_recent_matches(profile_id, min_matches=20, max_pages=4):
    """Fetch recent match history for a profile_id from the Relic API.
    Returns dicts newest-first: {match_id, map, civ, opponents (list[str]),
    result ('win'|'loss'|None), rating_delta (int|None), duration_min
    (float|None), ago_text, strategy (always None -- not in this API)}."""
    try:
        target_id = int(profile_id)
    except (TypeError, ValueError):
        LOGGER.warning("get_recent_matches got a non-numeric profile_id: %r", profile_id)
        return []

    try:
        data = _get("/community/leaderboard/getRecentMatchHistory", {
            "title": TITLE,
            "profile_ids": json.dumps([target_id]),
        })
    except (requests.RequestException, ValueError) as e:
        LOGGER.warning("match history request failed for profile %s: %s", target_id, e)
        return []

    stats = data.get("matchHistoryStats") or []
    if not stats:
        LOGGER.info("matches_debug no matchHistoryStats for profile %s; raw keys=%s", target_id, list(data.keys()))
        return []

    stats.sort(key=lambda m: m.get("startgametime") or 0, reverse=True)

    matches = []
    for m in stats[:min_matches]:
        results = m.get("matchhistoryreportresults") or []
        me = next((r for r in results if r.get("profile_id") == target_id), None)
        if me is None:
            continue

        civ = aoe2ref.civ_name(me.get("civilization_id"))
        opponents = []
        for r in results:
            if r is me:
                continue
            oc = aoe2ref.civ_name(r.get("civilization_id"))
            if oc:
                opponents.append(oc)

        resulttype = me.get("resulttype")
        result = "win" if resulttype == 1 else ("loss" if resulttype is not None else None)

        member = me.get("matchhistorymember") or {}
        old_r, new_r = member.get("oldrating"), member.get("newrating")
        rating_delta = (new_r - old_r) if (old_r is not None and new_r is not None) else None

        start, end = m.get("startgametime"), m.get("completiontime")
        duration_min = round((end - start) / 60, 1) if (start and end and end > start) else None

        matches.append({
            "match_id": m.get("id"),
            "map": _prettify_map(m.get("mapname")),
            "civ": civ,
            "opponents": opponents[:7],
            "result": result,
            "rating_delta": rating_delta,
            "duration_min": duration_min,
            "ago_text": _time_ago(start),
            "strategy": None,
        })

    return matches
