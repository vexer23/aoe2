"""Static AoE2 reference data (civ/tech/unit/building names).

Bundled from the `aocref` package's dataset 100 (the current DE dataset),
downloaded from PyPI at build time. See aocref_100.json.

Key facts (verified against the installed mgz-fast 1.0.0 source and this
dataset -- see project notes):
  - mgz-fast's `civilization_id` (from header['de']['players']) matches the
    OUTER KEY of this dataset's `civilizations` dict, not the inner "id"
    field.
  - Age-up technology ids: 101 Feudal, 102 Castle, 103 Imperial (104 Dark,
    unused since games start there).
  - Villager unit ids: 83 and 293 (both mean "Villager" -- DE has two ids
    for historical reasons).
"""
import json
import os

_REF_PATH = os.path.join(os.path.dirname(__file__), "aocref_100.json")

with open(_REF_PATH, "r", encoding="utf-8") as f:
    _DATA = json.load(f)

CIVILIZATIONS = _DATA["civilizations"]   # {"1": {"name": "Britons", "id": 5}, ...} -- key is mgz civilization_id
TECHNOLOGIES = _DATA["technologies"]     # {"101": "Feudal Age", ...}
OBJECTS = _DATA["objects"]               # {"83": "Villager", "4": "Archer", ...}
MAPS = _DATA["maps"]                     # {"9": "Arabia", ...}

# Sorted longest-first so substring matching against scraped text prefers the
# most specific name (e.g. "Black Forest" before "Forest").
CIV_NAMES = sorted({v["name"] for v in CIVILIZATIONS.values() if v.get("name")}, key=len, reverse=True)
MAP_NAMES = sorted({v for v in MAPS.values() if v}, key=len, reverse=True)

AGE_TECH_IDS = {101: "Feudal Age", 102: "Castle Age", 103: "Imperial Age"}
VILLAGER_UNIT_IDS = {83, 293}

# A curated set of "trunk line" + common unique military units, by object id,
# for build-order / military-composition display. Not exhaustive across all
# civs -- unrecognized unit ids still resolve via OBJECTS with a generic
# fallback, this set just marks what counts as "military" for summaries.
_MILITARY_KEYWORDS = (
    "man-at-arms", "swordsman", "champion", "longswordsman", "two-handed swordsman",
    "archer", "crossbowman", "arbalester", "skirmisher", "cavalry archer", "hand cannoneer",
    "scout", "light cavalry", "hussar", "knight", "cavalier", "paladin", "camel",
    "spearman", "pikeman", "halberdier",
    "mangonel", "onager", "siege ram", "battering ram", "capped ram", "scorpion", "bombard cannon", "trebuchet",
    "galley", "war galley", "fire ship", "demolition ship", "cannon galleon", "elite cannon galleon",
    "monk", "missionary",
)


def civ_name(civ_id):
    """mgz civilization_id -> display name."""
    entry = CIVILIZATIONS.get(str(civ_id))
    if entry:
        return entry.get("name", f"Civ {civ_id}")
    return f"Civ {civ_id}"


def tech_name(tech_id):
    return TECHNOLOGIES.get(str(tech_id)) or f"Tech {tech_id}"


def object_name(object_id):
    return OBJECTS.get(str(object_id)) or f"Unit {object_id}"


def is_villager(unit_id):
    return unit_id in VILLAGER_UNIT_IDS


def is_military(unit_id):
    name = (OBJECTS.get(str(unit_id)) or "").lower()
    return any(k in name for k in _MILITARY_KEYWORDS)
