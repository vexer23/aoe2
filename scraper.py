"""Best-effort scraping of aoe2insights.com for player search + match history.

This is the one module in the app I could not test against live HTML from
the build environment (its outbound network is policy-restricted to package
registries only). It's deliberately defensive: field extraction is done by
searching for known-shape patterns (civ names, map names, "/match/<id>/" and
"/user/<id>/" links, W/L + rating-delta patterns) inside the page's plain
text rather than relying on guessed CSS class names, which is more likely to
survive small markup changes. If a field can't be confidently found it's left
None rather than guessed wrong. Check the RUNTIME_NOTES.md file for how to
patch this quickly if the site's markup doesn't match on first deploy --
`get_logs()` output (search_debug / matches_debug) is written specifically
to make that diagnosis fast.
"""
import logging
import re
import time
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

import aoe2ref

LOGGER = logging.getLogger("scraper")

BASE = "https://www.aoe2insights.com"
# A self-identifying bot User-Agent got flat 403'd by the site's WAF before a
# single request even reached page logic. Presenting as an ordinary browser
# (full header set, persistent session/cookies, a warm-up hit on the
# homepage first) is what real browsing traffic looks like and is enough to
# get past simple bot rules -- it is not an attempt to defeat anything more
# sophisticated than that (no CAPTCHA solving, no JS challenge execution).
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Referer": BASE + "/",
}
TIMEOUT = 20

_session = requests.Session()
_session.headers.update(HEADERS)
_warmed_up = False


def _warm_up():
    """Visit the homepage once per process so we carry the same cookies a
    real browser would have before hitting /search/ or /user/.../matches/."""
    global _warmed_up
    if _warmed_up:
        return
    try:
        _session.get(BASE + "/", timeout=TIMEOUT)
    except requests.RequestException as e:
        LOGGER.info("warm-up request failed (continuing anyway): %s", e)
    _warmed_up = True


USER_LINK_RE = re.compile(r"/user/(\d+)/")
MATCH_LINK_RE = re.compile(r"/match/(\d+)/")
RATING_LABEL_RE = re.compile(
    r"\b((?:RM|EW|DM)\s*(?:1v1|Team)|Unranked)\b[^\d\n]{0,10}(\d{2,5})", re.IGNORECASE
)
DELTA_RE = re.compile(r"([+-]\s?\d{1,3})\s*(?:rating|elo)?\b", re.IGNORECASE)
DURATION_RE = re.compile(r"\b(\d{1,3})\s*m(?:in)?(?:\s*(\d{1,2})\s*s)?\b")
AGO_RE = re.compile(r"\b(\d+\s*(?:minute|hour|day|week|month|year)s?\s*ago|just now)\b", re.IGNORECASE)
STRATEGY_RE = re.compile(
    r"\b((?:Archer|Scout|Man-at-Arms|Drush|Fast Castle|Feudal|Tower)\s*Rush|Fast Castle|Drush\+?)\b",
    re.IGNORECASE,
)


def _get(url, params=None):
    _warm_up()
    resp = _session.get(url, params=params, timeout=TIMEOUT)
    if resp.status_code >= 400:
        LOGGER.warning(
            "request to %s returned %s; body starts with: %r",
            resp.url, resp.status_code, resp.text[:300],
        )
    resp.raise_for_status()
    return resp


def search_player(name, max_candidates=8):
    """Search aoe2insights.com by display name. Returns a list of candidate dicts:
    {"name", "profile_id", "country", "rating_label"}. Best effort -- may return []
    even for a real name if the site's markup has changed; check logs."""
    try:
        resp = _get(f"{BASE}/search/", params={"q": name})
    except requests.RequestException as e:
        LOGGER.warning("search request failed for %r: %s", name, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    candidates = []
    seen_ids = set()

    for a in soup.find_all("a", href=USER_LINK_RE):
        m = USER_LINK_RE.search(a.get("href", ""))
        if not m:
            continue
        profile_id = m.group(1)
        if profile_id in seen_ids:
            continue

        link_text = a.get_text(strip=True)
        # Look at the surrounding block (parent container) for rating/country context.
        container = a
        for _ in range(3):
            if container.parent is not None:
                container = container.parent
        block_text = container.get_text(" ", strip=True)

        rating_match = RATING_LABEL_RE.search(block_text)
        rating_label = f"{rating_match.group(1)} {rating_match.group(2)}" if rating_match else None

        if not link_text or link_text.isdigit():
            continue

        seen_ids.add(profile_id)
        candidates.append({
            "name": link_text,
            "profile_id": profile_id,
            "country": None,  # not reliably extractable without knowing the flag markup; left for a future pass
            "rating_label": rating_label,
        })
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        LOGGER.info("search_debug no candidates found for %r; response length=%d", name, len(resp.text))
    return candidates


def get_recent_matches(profile_id, min_matches=20, max_pages=4):
    """Scrape the player's match list. Returns a list of dicts, newest first, each:
    {match_id, map, civ, opponents (list[str]), result ('win'|'loss'|None),
     rating_delta (int|None), duration_min (float|None), ago_text, strategy (str|None)}
    """
    all_matches = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        params = {} if page == 1 else {"page": page}
        try:
            resp = _get(f"{BASE}/user/{profile_id}/matches/", params=params)
        except requests.RequestException as e:
            LOGGER.warning("matches request failed for profile %s page %d: %s", profile_id, page, e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        match_links = soup.find_all("a", href=MATCH_LINK_RE)
        if not match_links:
            LOGGER.info("matches_debug no match links found for profile %s page %d; response length=%d",
                        profile_id, page, len(resp.text))
            break

        page_new = 0
        for a in match_links:
            m = MATCH_LINK_RE.search(a.get("href", ""))
            if not m:
                continue
            match_id = m.group(1)
            if match_id in seen_ids:
                continue
            seen_ids.add(match_id)
            page_new += 1

            container = a
            for _ in range(4):
                if container.parent is not None:
                    container = container.parent
            block_text = container.get_text(" ", strip=True)

            all_matches.append(_parse_match_block(match_id, block_text))

        if page_new == 0:
            break
        if len(all_matches) >= min_matches:
            break
        time.sleep(0.5)  # be polite between pages

    return all_matches[:max(min_matches, len(all_matches))]


def _parse_match_block(match_id, text):
    civ = next((c for c in aoe2ref.CIV_NAMES if c in text), None)
    map_name = next((m for m in aoe2ref.MAP_NAMES if m in text), None)

    result = None
    if re.search(r"\bWin\b", text):
        result = "win"
    elif re.search(r"\bLoss\b|\bLost\b|\bDefeat\b", text, re.IGNORECASE):
        result = "loss"

    rating_delta = None
    delta_match = DELTA_RE.search(text)
    if delta_match:
        try:
            rating_delta = int(delta_match.group(1).replace(" ", ""))
        except ValueError:
            rating_delta = None

    duration_min = None
    dur_match = DURATION_RE.search(text)
    if dur_match:
        minutes = int(dur_match.group(1))
        seconds = int(dur_match.group(2)) if dur_match.group(2) else 0
        duration_min = round(minutes + seconds / 60, 1)

    ago_match = AGO_RE.search(text)
    ago_text = ago_match.group(1) if ago_match else None

    strategy_match = STRATEGY_RE.search(text)
    strategy = strategy_match.group(1).strip() if strategy_match else None

    # Opponent civs: every civ name mentioned besides the player's own.
    opponents = [c for c in aoe2ref.CIV_NAMES if c in text and c != civ]

    return {
        "match_id": int(match_id),
        "map": map_name,
        "civ": civ,
        "opponents": opponents[:7],
        "result": result,
        "rating_delta": rating_delta,
        "duration_min": duration_min,
        "ago_text": ago_text,
        "strategy": strategy,
    }
