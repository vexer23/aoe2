"""Orchestrates: name search resolution, fetching a tracked player's recent
matches, downloading + parsing replays, and computing the aggregate stats
the dashboard reads. Runs both on-demand (Flask request handlers call these
directly) and on a background timer thread (see start_background_loop)."""
import io
import logging
import threading
import time
import zipfile
from datetime import datetime, timezone

import requests

import aoe2ref
import replay_parser
import scraper
import storage

LOGGER = logging.getLogger("pipeline")

REPLAY_DOWNLOAD_URL = "https://aoe.ms/replay/"
REFRESH_INTERVAL_SECONDS = 20 * 60  # background loop cadence
MIN_MATCHES = 20

_refresh_lock = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- search ---

def submit_search(query):
    storage.kv_set("search_current", {
        "query": query,
        "status": "pending",
        "requested_at": now_iso(),
    })
    # Resolve inline for responsiveness; the background loop is a fallback
    # in case this request is interrupted mid-flight.
    threading.Thread(target=resolve_pending_search, daemon=True).start()


def resolve_pending_search():
    current = storage.kv_get("search_current")
    if not current or current.get("status") != "pending":
        return
    query = current["query"]
    try:
        candidates = scraper.search_player(query)
    except Exception as e:  # noqa: BLE001
        LOGGER.exception("search failed for %r", query)
        storage.kv_set("search_current", {
            **current, "status": "error", "error_message": str(e), "resolved_at": now_iso(),
        })
        return

    if candidates:
        storage.kv_set("search_current", {
            **current, "status": "resolved", "candidates": candidates, "resolved_at": now_iso(),
        })
    else:
        storage.kv_set("search_current", {
            **current, "status": "error", "error_message": "No matching profiles found",
            "candidates": [], "resolved_at": now_iso(),
        })


def confirm_player(candidate):
    storage.kv_set("tracked_player", {
        "name": candidate["name"],
        "profile_id": candidate["profile_id"],
        "rating_label": candidate.get("rating_label"),
        "status": "confirmed",
        "confirmed_at": now_iso(),
    })
    storage.kv_set("status", {"phase": "fetching", "message": f"Tracking {candidate['name']}, fetching matches…", "updated_at": now_iso()})
    threading.Thread(target=refresh_tracked_player, daemon=True).start()


def untrack_player():
    storage.kv_set("tracked_player", {"status": "none"})
    storage.kv_set("search_current", None)
    storage.kv_set("stats_summary", None)


# ------------------------------------------------------------- replays -----

def download_replay(match_id, profile_id):
    """Returns raw .aoe2record bytes, or None on failure. The download is
    sometimes a zip containing the record, sometimes the raw file directly --
    handle both (per the official support article's "unzip the downloaded
    replay folder" instruction)."""
    try:
        resp = requests.get(
            REPLAY_DOWNLOAD_URL,
            params={"gameId": match_id, "profileId": profile_id},
            timeout=60,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        LOGGER.warning("replay download failed for match %s: %s", match_id, e)
        return None

    content = resp.content
    if content[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                names = [n for n in zf.namelist() if n.lower().endswith((".aoe2record", ".mgz"))]
                if not names:
                    names = zf.namelist()[:1]
                if not names:
                    return None
                return zf.read(names[0])
        except zipfile.BadZipFile:
            LOGGER.warning("replay for match %s looked like a zip but wasn't valid", match_id)
            return None
    return content


# ------------------------------------------------------------ refresh ------

def refresh_tracked_player(force=False):
    if not _refresh_lock.acquire(blocking=False):
        LOGGER.info("refresh already in progress, skipping")
        return
    try:
        _do_refresh(force=force)
    finally:
        _refresh_lock.release()


def _do_refresh(force=False):
    tracked = storage.kv_get("tracked_player")
    if not tracked or tracked.get("status") != "confirmed":
        return

    profile_id = tracked["profile_id"]
    storage.kv_set("status", {"phase": "fetching", "message": f"Fetching matches for {tracked['name']}…", "updated_at": now_iso()})

    try:
        scraped_matches = scraper.get_recent_matches(profile_id, min_matches=MIN_MATCHES)
    except Exception as e:  # noqa: BLE001
        LOGGER.exception("match list scrape failed")
        storage.kv_set("status", {"phase": "error", "message": f"Couldn't fetch match list: {e}", "updated_at": now_iso()})
        return

    if not scraped_matches:
        storage.kv_set("status", {"phase": "error", "message": "No matches found for this profile", "updated_at": now_iso()})
        return

    for m in scraped_matches:
        existing = storage.get_match(m["match_id"])
        existing_parsed = (existing or {}).get("parsed")
        # Reparse if: forced, never seen before, never successfully parsed
        # and not permanently failed either, OR it was parsed by an older
        # version of replay_parser.py -- this makes a parsing-logic fix
        # (e.g. a villager-count bug) self-heal already-cached matches on
        # the next refresh, without needing a manual force-refresh.
        stale_version = existing_parsed is not None and existing_parsed.get("parser_version") != replay_parser.PARSER_VERSION
        needs_parse = (
            force or existing is None or stale_version or
            (existing.get("parsed") is None and existing.get("parse_error") is None)
        )
        storage.upsert_match(m["match_id"], scraped=m)

        if not needs_parse:
            continue

        raw = download_replay(m["match_id"], profile_id)
        if raw is None:
            storage.upsert_match(m["match_id"], parse_error="replay download failed (may not be available yet)")
            continue

        replay_path = storage.replay_path_for(m["match_id"])
        try:
            with open(replay_path, "wb") as f:
                f.write(raw)
        except OSError as e:
            LOGGER.warning("could not save replay for match %s: %s", m["match_id"], e)
            replay_path = None

        try:
            parsed_full = replay_parser.parse_replay(raw)
        except replay_parser.ReplayParseError as e:
            LOGGER.warning("parse failed for match %s: %s", m["match_id"], e)
            storage.upsert_match(m["match_id"], replay_path=replay_path, parse_error=str(e))
            continue

        me = _find_player_by_profile(parsed_full, profile_id)
        if me is None:
            storage.upsert_match(m["match_id"], replay_path=replay_path, parse_error="tracked player not found in replay")
            continue

        outcome = replay_parser.infer_outcome(parsed_full)
        me_result = outcome.get(me["number"], "unknown")
        opponents_detail = [
            {"name": p["name"], "civ": p["civ_name"], "number": p["number"]}
            for p in parsed_full["players"] if p["number"] != me["number"]
        ]
        parsed_summary = {
            "parser_version": parsed_full.get("parser_version"),
            "duration_ms": parsed_full["duration_ms"],
            "civ": me["civ_name"],
            "age_ups": me["age_ups"],
            "villager_timeline": me["villager_timeline"],
            "final_villager_count": me["final_villager_count"],
            "first_military_unit": me["first_military_unit"],
            "military_event_count": me["military_event_count"],
            "build_order": me["build_order"],
            "outcome_inferred": me_result,
            "opponents_detail": opponents_detail,
        }
        storage.upsert_match(m["match_id"], replay_path=replay_path, parsed=parsed_summary, parse_error=None)
        time.sleep(0.3)  # be gentle on aoe.ms between downloads

    storage.kv_set("status", {"phase": "computing", "message": "Computing stats…", "updated_at": now_iso()})
    _compute_and_store_stats(tracked)
    storage.kv_set("status", {"phase": "done", "message": f"Refreshed {tracked['name']}'s last {len(scraped_matches)} games", "updated_at": now_iso()})


def _find_player_by_profile(parsed_full, profile_id):
    try:
        target = int(profile_id)
    except (TypeError, ValueError):
        return None
    number = None
    for p in parsed_full["players"]:
        if p.get("profile_id") == target:
            number = p["number"]
            break
    if number is None:
        return None
    return parsed_full["per_player"].get(number)


# --------------------------------------------------------- aggregation -----

def _compute_and_store_stats(tracked):
    matches = storage.list_matches(limit=MIN_MATCHES)
    matches.sort(key=lambda m: m["match_id"])  # oldest -> newest for the trend series

    wins = losses = 0
    civ_counter = {}
    map_counter = {}
    duration_buckets = {"Under 20 min": [0, 0], "20-40 min": [0, 0], "40-60 min": [0, 0], "60+ min": [0, 0]}
    strategy_counter = {}
    rating_series = []
    recent_matches = []
    age_up_series = []       # [{label, feudal_min, castle_min, imperial_min}]
    villager_series = []     # [{label, villagers_at_10, villagers_at_20, final}]
    first_military_series = []  # [{label, unit, minute}]
    running_rating = None

    for m in matches:
        scraped = m.get("scraped") or {}
        parsed = m.get("parsed")
        result = (parsed or {}).get("outcome_inferred")
        if result not in ("win", "loss"):
            result = scraped.get("result")
        if result == "win":
            wins += 1
        elif result == "loss":
            losses += 1

        civ = (parsed or {}).get("civ") or scraped.get("civ")
        if civ:
            entry = civ_counter.setdefault(civ, {"games": 0, "wins": 0})
            entry["games"] += 1
            if result == "win":
                entry["wins"] += 1

        map_name = scraped.get("map")
        if map_name:
            entry = map_counter.setdefault(map_name, {"games": 0, "wins": 0})
            entry["games"] += 1
            if result == "win":
                entry["wins"] += 1

        duration_min = scraped.get("duration_min")
        if duration_min is None and parsed:
            duration_min = round(parsed["duration_ms"] / 60000, 1)
        if duration_min is not None:
            bucket = ("Under 20 min" if duration_min < 20 else
                      "20-40 min" if duration_min < 40 else
                      "40-60 min" if duration_min < 60 else "60+ min")
            duration_buckets[bucket][0] += 1
            if result == "win":
                duration_buckets[bucket][1] += 1

        strategy = scraped.get("strategy")
        if strategy:
            entry = strategy_counter.setdefault(strategy, {"games": 0, "wins": 0})
            entry["games"] += 1
            if result == "win":
                entry["wins"] += 1

        delta = scraped.get("rating_delta")
        if delta is not None:
            running_rating = delta if running_rating is None else running_rating + delta
            rating_series.append({
                "rating": running_rating, "delta": delta,
                "result": result or "unknown", "label": map_name or civ or f"Game {m['match_id']}",
            })

        label = map_name or (civ or "")
        if parsed and parsed.get("age_ups"):
            age_ups = parsed["age_ups"]
            age_up_series.append({
                "label": label,
                "feudal_min": _ms_to_min(age_ups.get("Feudal Age")),
                "castle_min": _ms_to_min(age_ups.get("Castle Age")),
                "imperial_min": _ms_to_min(age_ups.get("Imperial Age")),
            })
        if parsed and parsed.get("villager_timeline"):
            timeline = parsed["villager_timeline"]
            villager_series.append({
                "label": label,
                "at_10min": _count_at_minute(timeline, 10),
                "at_20min": _count_at_minute(timeline, 20),
                "final": parsed.get("final_villager_count"),
            })
        if parsed and parsed.get("first_military_unit"):
            fm = parsed["first_military_unit"]
            first_military_series.append({
                "label": label, "unit": fm["unit"], "minute": round(fm["t_ms"] / 60000, 1),
            })

        recent_matches.append({
            "match_id": m["match_id"],
            "date": scraped.get("ago_text"),
            "civ": civ,
            "map": map_name,
            "opponents": [o["civ"] for o in (parsed or {}).get("opponents_detail", [])] or scraped.get("opponents", []),
            "result": result,
            "ratingDelta": delta,
            "duration": f"{duration_min}m" if duration_min is not None else None,
            "strategy": strategy,
            "has_replay_analysis": bool(parsed),
            "parse_error": m.get("parse_error"),
        })

    recent_matches.reverse()  # newest first for display

    total = wins + losses
    suggestions = _build_suggestions(civ_counter, map_counter, rating_series, age_up_series, total)

    summary = {
        "player_name": tracked["name"],
        "totalGames": total,
        "wins": wins,
        "losses": losses,
        "currentRating": rating_series[-1]["rating"] if rating_series else None,
        "civStats": _counter_to_rows(civ_counter),
        "mapStats": _counter_to_rows(map_counter),
        "durationStats": [{"label": k, "games": v[0], "wins": v[1]} for k, v in duration_buckets.items() if v[0] > 0],
        "strategyStats": _counter_to_rows(strategy_counter),
        "ratingSeries": rating_series,
        "ageUpSeries": age_up_series,
        "villagerSeries": villager_series,
        "firstMilitarySeries": first_military_series,
        "recentMatches": recent_matches,
        "suggestions": suggestions,
        "replaysAnalyzed": sum(1 for m in matches if m.get("parsed")),
        "computedAt": now_iso(),
    }
    storage.kv_set("stats_summary", summary)


def _counter_to_rows(counter):
    rows = [{"label": k, "games": v["games"], "wins": v["wins"]} for k, v in counter.items()]
    rows.sort(key=lambda r: r["games"], reverse=True)
    return rows


def _ms_to_min(ms):
    return round(ms / 60000, 1) if ms is not None else None


def _count_at_minute(timeline, minute):
    target_ms = minute * 60000
    count = 0
    for point in timeline:
        if point["t_ms"] <= target_ms:
            count = point["count"]
        else:
            break
    return count


def _build_suggestions(civ_counter, map_counter, rating_series, age_up_series, total):
    tips = []
    if total < 3:
        tips.append("Not enough recent games yet for reliable trends -- check back after a few more matches.")
        return tips

    worst_map = min(
        (v for v in map_counter.values() if v["games"] >= 3),
        key=lambda v: v["wins"] / v["games"], default=None
    )
    if worst_map:
        name = next(k for k, v in map_counter.items() if v is worst_map)
        pct = round(100 * worst_map["wins"] / worst_map["games"])
        tips.append(f"{name} has been a weak map lately: {worst_map['wins']}-{worst_map['games']-worst_map['wins']} ({pct}% win rate) over {worst_map['games']} games.")

    if len(rating_series) >= 6:
        last5 = sum(p["delta"] for p in rating_series[-5:])
        prev5 = sum(p["delta"] for p in rating_series[-10:-5]) if len(rating_series) >= 10 else None
        if last5 < 0 and (prev5 is None or last5 < prev5):
            tips.append(f"Rating is trending down over the last 5 games (net {last5:+d}). Worth a short break or reviewing recent replays before queuing more.")
        elif last5 > 0:
            tips.append(f"Rating is trending up over the last 5 games (net {last5:+d}) -- whatever's changed recently seems to be working.")

    if age_up_series:
        feudal_times = [a["feudal_min"] for a in age_up_series if a["feudal_min"] is not None]
        if len(feudal_times) >= 3:
            avg_feudal = sum(feudal_times) / len(feudal_times)
            worst = max(feudal_times)
            if worst > avg_feudal * 1.3:
                tips.append(f"Feudal Age timing varies a lot game to game (avg {avg_feudal:.1f} min, slowest {worst:.1f} min) -- a more consistent opening build would tighten that up.")

    best_civ = max((v for v in civ_counter.values() if v["games"] >= 3), key=lambda v: v["wins"] / v["games"], default=None)
    if best_civ:
        name = next(k for k, v in civ_counter.items() if v is best_civ)
        tips.append(f"{name} has been your strongest civ recently ({best_civ['wins']}-{best_civ['games']-best_civ['wins']}) -- leaning on it more could be an easy rating boost.")

    if not tips:
        tips.append("No strong patterns yet in this sample -- keep playing and check back as more games come in.")
    return tips[:5]


# ------------------------------------------------------------ scheduler ----

def start_background_loop():
    def loop():
        while True:
            try:
                resolve_pending_search()
                refresh_tracked_player()
            except Exception:  # noqa: BLE001
                LOGGER.exception("background loop iteration failed")
            time.sleep(REFRESH_INTERVAL_SECONDS)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t
