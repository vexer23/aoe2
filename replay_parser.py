"""Parse an AoE2:DE .aoe2record replay into a structured per-player summary.

Built on `mgz-fast` (the same fork used in earlier iterations of this
project on real DE replays -- see project notes on its header quirks).

Ground truth for the format, verified directly against the installed
mgz-fast 1.0.0 source (not guessed):
  - File layout: [4B header_length][4B chapter_address][zlib header][body].
    `mgz.fast.header.parse(io.BytesIO(raw))` consumes exactly the header
    portion and leaves the same stream positioned at the start of the body.
  - Body: call `mgz.fast.meta(data)` once, then loop `mgz.fast.operation(data)`
    until it raises EOFError. Each call returns (Operation, payload):
      Operation.SYNC   -> payload is (increment, checksum, sync_dict); an
                          absolute "current_time" (ms since game start) is
                          present on the DE checksum-sync variant, otherwise
                          `increment` is a ms delta to accumulate.
      Operation.ACTION -> payload is (Action, action_dict); action_dict has
                          player_id plus type-specific fields.
      Operation.POSTGAME -> payload is {'leaderboards': [...], ...}.
  - Player civ ids: header['de']['players'] is the reliable, actively
    maintained list (profile_id, civilization_id, name, number). The plain
    header['players'] list can have a stale/wrong civilization_id on very
    new save versions -- always prefer header['de']['players'].
  - civilization_id indexes aoe2ref.CIVILIZATIONS by OUTER KEY, not the
    inner "id" field.
  - Age-up techs: 101/102/103 = Feudal/Castle/Imperial (mgz.fast.actions
    Action.RESEARCH -> technology_id).
  - Villager unit ids 83 and 293 (Action.MAKE / Action.DE_QUEUE -> unit_id).
  - Action.BUILD -> building_id (an object id, resolved via aoe2ref).
  - Action.RESIGN -> player_id of the player who resigned. AoE2:DE replays
    carry no explicit "winner" field -- outcome must be inferred from which
    team's players resigned.
"""
import io
import logging

from mgz.fast import header as fast_header
from mgz.fast import meta, operation
from mgz.fast.enums import Operation as Op
from mgz.fast.enums import Action

import aoe2ref

LOGGER = logging.getLogger("replay_parser")

BUILD_ORDER_CAP_MS = 20 * 60 * 1000  # first 20 minutes, matches earlier project scope

# Bump this whenever parsing logic changes in a way that affects the output
# (not just bugfixes to error handling). pipeline.py compares this against
# the value stored on an already-parsed match and reparses it if they don't
# match, so a fix here self-heals previously-cached matches on the next
# refresh instead of requiring a manual force-refresh.
PARSER_VERSION = 2


class ReplayParseError(Exception):
    pass


def parse_replay(raw_bytes):
    """Parse raw .aoe2record bytes into a structured dict. Raises ReplayParseError."""
    data = io.BytesIO(raw_bytes)
    try:
        header = fast_header.parse(data)
    except Exception as e:  # noqa: BLE001 - replay parsing is inherently fragile, never crash the pipeline
        raise ReplayParseError(f"header parse failed: {e}") from e

    de = header.get("de") or {}
    de_players = de.get("players") or []
    if not de_players:
        # Fall back to the general path if this save version has no DE block for some reason.
        de_players = header.get("players") or []

    players_by_number = {}
    for p in de_players:
        number = p.get("number")
        if number is None:
            continue
        players_by_number[number] = {
            "number": number,
            "profile_id": p.get("profile_id"),
            "name": _decode(p.get("name")),
            "team_id": p.get("team_id"),
            "civ_id": p.get("civilization_id"),
            "civ_name": aoe2ref.civ_name(p.get("civilization_id")),
        }

    per_player = {n: _new_player_state() for n in players_by_number}

    try:
        meta(data)
    except Exception as e:  # noqa: BLE001
        raise ReplayParseError(f"body meta read failed: {e}") from e

    current_time_ms = 0
    leaderboards = []
    op_count = 0
    error_count = 0

    while True:
        try:
            op_type, result = operation(data)
        except EOFError:
            break
        except Exception as e:  # noqa: BLE001
            # A single corrupt operation shouldn't sink the whole replay.
            error_count += 1
            if error_count > 500:
                LOGGER.warning("too many body parse errors, stopping early: %s", e)
                break
            continue

        op_count += 1

        if op_type == Op.SYNC:
            increment, checksum, sync_payload = result
            if isinstance(sync_payload, dict) and "current_time" in sync_payload:
                current_time_ms = sync_payload["current_time"]
            elif increment:
                current_time_ms += increment
            continue

        if op_type == Op.POSTGAME:
            if isinstance(result, dict) and result.get("leaderboards"):
                leaderboards = result["leaderboards"]
            continue

        if op_type != Op.ACTION:
            continue

        action_type, action_payload = result
        player_id = action_payload.get("player_id")
        state = per_player.get(player_id)
        if state is None:
            continue

        _apply_action(state, action_type, action_payload, current_time_ms)

    duration_ms = current_time_ms

    if error_count:
        LOGGER.info(
            "replay parsed with %d/%d operation errors (save_version=%s) -- "
            "a high error count usually means this save version has action "
            "formats mgz-fast doesn't fully know about yet, which can silently "
            "undercount things like villagers",
            error_count, op_count + error_count, header.get("save_version"),
        )

    result = {
        "parser_version": PARSER_VERSION,
        "save_version": header.get("save_version"),
        "duration_ms": duration_ms,
        "players": list(players_by_number.values()),
        "per_player": {
            n: _finalize_player(state, players_by_number[n])
            for n, state in per_player.items()
        },
        "leaderboards": leaderboards,
        "op_count": op_count,
        "parse_errors": error_count,
    }
    return result


def _decode(value):
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace").strip("\x00").strip()
        except Exception:  # noqa: BLE001
            return value.hex()
    return value


def _new_player_state():
    return {
        "villager_events": [],   # (t_ms, unit_id, amount) -- amount is how many this one action queued
        "age_up_events": {},     # tech_name -> t_ms (first occurrence)
        "military_events": [],   # (t_ms, unit_id, unit_name) -- one entry per action, not per unit produced
        "building_events": [],   # (t_ms, building_id, building_name)
        "resigned_at_ms": None,
    }


def _apply_action(state, action_type, payload, t_ms):
    if action_type in (Action.MAKE, Action.DE_QUEUE):
        unit_id = payload.get("unit_id")
        if unit_id is None:
            return
        # DE_QUEUE carries an `amount` field -- a single queue action can
        # enqueue more than one unit at once (e.g. queuing several villagers
        # in one click), and ignoring it silently undercounts production.
        # MAKE has no such field and always represents exactly one unit.
        amount = payload.get("amount", 1) if action_type == Action.DE_QUEUE else 1
        if not isinstance(amount, int) or amount < 1 or amount > 50:
            amount = 1  # defensive: a garbage/out-of-range value shouldn't corrupt the count
        if aoe2ref.is_villager(unit_id):
            state["villager_events"].append((t_ms, unit_id, amount))
        elif aoe2ref.is_military(unit_id):
            state["military_events"].append((t_ms, unit_id, aoe2ref.object_name(unit_id)))
        return

    if action_type == Action.RESEARCH:
        tech_id = payload.get("technology_id")
        if tech_id in aoe2ref.AGE_TECH_IDS:
            name = aoe2ref.AGE_TECH_IDS[tech_id]
            state["age_up_events"].setdefault(name, t_ms)
        return

    if action_type == Action.BUILD:
        building_id = payload.get("building_id")
        if building_id is None:
            return
        state["building_events"].append((t_ms, building_id, aoe2ref.object_name(building_id)))
        return

    if action_type == Action.RESIGN:
        if state["resigned_at_ms"] is None:
            state["resigned_at_ms"] = t_ms
        return


def _finalize_player(state, info):
    villager_events = sorted(state["villager_events"])
    military_events = sorted(state["military_events"])
    building_events = sorted(state["building_events"])

    # Cumulative villager count over time (each queue event adds its `amount`,
    # not always 1 -- see _apply_action). This still only counts villagers
    # ever produced, not villagers currently alive (it doesn't subtract
    # deaths), and doesn't include the handful of villagers a player starts
    # the game with (those exist from the start, never "queued").
    villager_timeline = []
    count = 0
    for t_ms, _unit_id, amount in villager_events:
        count += amount
        villager_timeline.append({"t_ms": t_ms, "count": count})

    first_military = None
    if military_events:
        t_ms, unit_id, name = military_events[0]
        first_military = {"t_ms": t_ms, "unit": name, "unit_id": unit_id}

    # First-seen timestamp per distinct military unit type (for composition trends).
    first_seen_by_unit = {}
    for t_ms, unit_id, name in military_events:
        first_seen_by_unit.setdefault(name, t_ms)

    build_order = []
    for t_ms, building_id, name in building_events:
        if t_ms <= BUILD_ORDER_CAP_MS:
            build_order.append({"t_ms": t_ms, "type": "building", "name": name})
    for name, t_ms in state["age_up_events"].items():
        if t_ms <= BUILD_ORDER_CAP_MS:
            build_order.append({"t_ms": t_ms, "type": "age", "name": name})
    build_order.sort(key=lambda e: e["t_ms"])

    return {
        "number": info["number"],
        "profile_id": info["profile_id"],
        "name": info["name"],
        "civ_name": info["civ_name"],
        "age_ups": state["age_up_events"],
        "villager_timeline": villager_timeline,
        "final_villager_count": count,
        "first_military_unit": first_military,
        "military_unit_first_seen": first_seen_by_unit,
        "military_event_count": len(military_events),
        "build_order": build_order,
        "resigned_at_ms": state["resigned_at_ms"],
    }


def infer_outcome(parsed):
    """Return {player_number: 'win'|'loss'|'unknown'} inferred from resignations
    and team assignments (DE replays carry no explicit winner field)."""
    players = parsed["players"]
    per_player = parsed["per_player"]
    teams = {}
    for p in players:
        teams.setdefault(p["team_id"], []).append(p["number"])

    resigned_teams = set()
    for number, pdata in per_player.items():
        if pdata.get("resigned_at_ms") is not None:
            player = next((p for p in players if p["number"] == number), None)
            if player is not None:
                resigned_teams.add(player["team_id"])

    outcome = {}
    all_teams = set(teams.keys())
    if resigned_teams and resigned_teams != all_teams:
        winning_teams = all_teams - resigned_teams
        for team_id, numbers in teams.items():
            result = "win" if team_id in winning_teams else "loss"
            for n in numbers:
                outcome[n] = result
    else:
        for p in players:
            outcome[p["number"]] = "unknown"
    return outcome
