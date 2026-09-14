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
    storage.kv_set("last_game_report", None)


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

        opponents_detail = []
        for p in parsed_full["players"]:
            if p["number"] == me["number"]:
                continue
            opp_state = parsed_full["per_player"].get(p["number"], {})
            opponents_detail.append({
                "name": p["name"], "civ": p["civ_name"], "number": p["number"],
                # Enough of the opponent's own timeline to compare against
                # (not their full build order -- keeps stored matches small).
                "age_ups": opp_state.get("age_ups", {}),
                "villagers_at_10": _count_at_minute(opp_state.get("villager_timeline") or [], 10),
                "villagers_at_20": _count_at_minute(opp_state.get("villager_timeline") or [], 20),
                "final_villager_count": opp_state.get("final_villager_count"),
                "first_military_unit": opp_state.get("first_military_unit"),
            })

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
            "resigned_at_ms": me["resigned_at_ms"],
            "opponents_detail": opponents_detail,
        }
        storage.upsert_match(m["match_id"], replay_path=replay_path, parsed=parsed_summary, parse_error=None)
        time.sleep(0.3)  # be gentle on aoe.ms between downloads

    storage.kv_set("status", {"phase": "computing", "message": "Computing stats…", "updated_at": now_iso()})
    _compute_and_store_stats(tracked)
    _build_last_game_report()
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


def _fmt_mmss(ms):
    if ms is None:
        return None
    total_s = int(round(ms / 1000))
    m, s = divmod(total_s, 60)
    return f"{m}:{s:02d}"


def _avg(values):
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def _build_last_game_report():
    """Deep-dive on the single most recent match with a successfully parsed
    replay: age-up pacing vs. the opponents in that same game, economy
    checkpoints vs. them, the biggest idle-production gap, first-military
    timing, and a short plain-language verdict. This is genuinely different
    from the aggregate civ/map/duration stats above -- it's about one game,
    compared against the actual opponents who were in it (also parsed from
    the same replay file), not against the field in general. Recomputed on
    every refresh from whatever match is currently newest, so it always
    reflects the latest game once its replay has been analyzed."""
    matches = storage.list_matches(limit=MIN_MATCHES)
    parsed_matches = [m for m in matches if m.get("parsed")]
    if not parsed_matches:
        storage.kv_set("last_game_report", None)
        return

    latest = max(parsed_matches, key=lambda m: m["match_id"])
    p = latest["parsed"]
    scraped = latest.get("scraped") or {}
    opponents = p.get("opponents_detail") or []

    def opp_values(key_fn):
        return [key_fn(o) for o in opponents]

    age_up_gaps = []
    for age_name in ("Feudal Age", "Castle Age", "Imperial Age"):
        mine = (p.get("age_ups") or {}).get(age_name)
        if mine is None:
            continue
        opp_avg = _avg(opp_values(lambda o: (o.get("age_ups") or {}).get(age_name)))
        delta_ms = (mine - opp_avg) if opp_avg is not None else None
        age_up_gaps.append({
            "age": age_name, "mine_ms": mine, "mine": _fmt_mmss(mine),
            "opponent_avg_ms": opp_avg,
            "opponent_avg": _fmt_mmss(opp_avg) if opp_avg is not None else None,
            "delta_ms": delta_ms,
        })

    villagers_10_opp = _avg(opp_values(lambda o: o.get("villagers_at_10")))
    villagers_20_opp = _avg(opp_values(lambda o: o.get("villagers_at_20")))
    my_10 = _count_at_minute(p.get("villager_timeline") or [], 10)
    my_20 = _count_at_minute(p.get("villager_timeline") or [], 20)

    # Biggest gap in villager production in the first 25 minutes -- a long
    # stretch with no new villager queued usually means an idle Town Center
    # (or a fight/raid that pulled full attention away from the economy).
    timeline = p.get("villager_timeline") or []
    biggest_gap = None
    CUTOFF_MS = 25 * 60 * 1000
    GAP_THRESHOLD_MS = 45_000
    if timeline and timeline[0]["t_ms"] >= GAP_THRESHOLD_MS:
        biggest_gap = {"start_ms": 0, "end_ms": timeline[0]["t_ms"], "gap_ms": timeline[0]["t_ms"]}
    for i in range(1, len(timeline)):
        prev_t, cur_t = timeline[i - 1]["t_ms"], timeline[i]["t_ms"]
        if cur_t > CUTOFF_MS:
            break
        gap = cur_t - prev_t
        if gap >= GAP_THRESHOLD_MS and (biggest_gap is None or gap > biggest_gap["gap_ms"]):
            biggest_gap = {"start_ms": prev_t, "end_ms": cur_t, "gap_ms": gap}

    my_first_mil = p.get("first_military_unit")
    opp_first_mil_ms = _avg(opp_values(lambda o: (o.get("first_military_unit") or {}).get("t_ms")))

    result = p.get("outcome_inferred")
    if result not in ("win", "loss"):
        result = scraped.get("result")
    resigned_at_ms = p.get("resigned_at_ms")

    # ---- plain-language narrative --------------------------------------
    lines = []
    opp_names = ", ".join(o["civ"] for o in opponents if o.get("civ")) or "your opponent(s)"
    outcome_word = {"win": "won", "loss": "lost"}.get(result, "finished")
    duration_label = _fmt_mmss(p.get("duration_ms"))
    opener = f"You {outcome_word} this {duration_label} game as {p.get('civ') or 'your civ'} vs {opp_names}"
    opener += f", resigning at {_fmt_mmss(resigned_at_ms)}." if resigned_at_ms else "."
    lines.append(opener)

    feudal = next((a for a in age_up_gaps if a["age"] == "Feudal Age"), None)
    if feudal and feudal["delta_ms"] is not None:
        if feudal["delta_ms"] > 30_000:
            lines.append(
                f"Feudal Age took {feudal['mine']}, about {_fmt_mmss(abs(feudal['delta_ms']))} slower than "
                f"the opponent average ({feudal['opponent_avg']}) -- the game was already trending behind from here."
            )
        elif feudal["delta_ms"] < -20_000:
            lines.append(
                f"Feudal Age came at {feudal['mine']}, {_fmt_mmss(abs(feudal['delta_ms']))} ahead of the "
                f"opponent average ({feudal['opponent_avg']}) -- a strong opening."
            )
        else:
            lines.append(f"Feudal Age at {feudal['mine']} was roughly in line with the opponent average ({feudal['opponent_avg']}).")

    if villagers_10_opp is not None:
        vill_delta = my_10 - villagers_10_opp
        if vill_delta <= -3:
            lines.append(
                f"At 10 minutes you had {my_10} villagers against an opponent average of {villagers_10_opp:.0f} -- an early economic deficit."
            )
        elif vill_delta >= 3:
            lines.append(
                f"At 10 minutes you had {my_10} villagers against an opponent average of {villagers_10_opp:.0f} -- ahead on economy early."
            )

    if biggest_gap:
        lines.append(
            f"The biggest gap in villager production was {round(biggest_gap['gap_ms']/1000)}s long, "
            f"from {_fmt_mmss(biggest_gap['start_ms'])} to {_fmt_mmss(biggest_gap['end_ms'])} -- "
            f"worth checking the replay there for an idle Town Center or a distracting fight."
        )

    if my_first_mil and opp_first_mil_ms is not None:
        mil_delta = my_first_mil["t_ms"] - opp_first_mil_ms
        if mil_delta > 45_000:
            lines.append(
                f"Your first military unit ({my_first_mil['unit']}) wasn't out until {_fmt_mmss(my_first_mil['t_ms'])}, "
                f"noticeably later than the opponent average ({_fmt_mmss(opp_first_mil_ms)}) -- military production started late."
            )

    if len(lines) == 1:
        lines.append("Nothing in this game stands out as a single deciding moment against these opponents -- a close, even game.")

    report = {
        "match_id": latest["match_id"],
        "civ": p.get("civ"),
        "opponents": [{"civ": o.get("civ"), "name": o.get("name")} for o in opponents],
        "result": result,
        "duration_ms": p.get("duration_ms"),
        "resigned_at_ms": resigned_at_ms,
        "ageUpComparison": age_up_gaps,
        "villagerComparison": {
            "mine_at_10": my_10, "opponent_avg_at_10": villagers_10_opp,
            "mine_at_20": my_20, "opponent_avg_at_20": villagers_20_opp,
        },
        "biggestIdleGap": biggest_gap,
        "firstMilitary": {"mine": my_first_mil, "opponent_avg_ms": opp_first_mil_ms},
        "narrative": lines,
        "computedAt": now_iso(),
    }
    storage.kv_set("last_game_report", report)


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
