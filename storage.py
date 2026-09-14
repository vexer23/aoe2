"""SQLite persistence. Single-user app, so this stays intentionally simple:
a key-value table for config/status, and a matches table keyed by match id.

DB_PATH should point at a Railway volume mount (e.g. /data/app.db) so it
survives redeploys; see app.py.
"""
import json
import os
import sqlite3
import threading
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", "/data/app.db")
REPLAY_DIR = os.environ.get("REPLAY_DIR", "/data/replays")

_lock = threading.Lock()


def init():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(REPLAY_DIR, exist_ok=True)
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY,
                scraped_json TEXT,
                replay_path TEXT,
                parsed_json TEXT,
                parse_error TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def kv_get(key, default=None):
    with _lock, _connect() as conn:
        row = conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except (TypeError, ValueError):
            return row["value"]


def kv_set(key, value):
    payload = json.dumps(value)
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO kv (key, value, updated_at) VALUES (?, ?, datetime('now'))
               ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')""",
            (key, payload),
        )
        conn.commit()


def upsert_match(match_id, scraped=None, replay_path=None, parsed=None, parse_error=None):
    with _lock, _connect() as conn:
        existing = conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        scraped_json = json.dumps(scraped) if scraped is not None else (existing["scraped_json"] if existing else None)
        replay_path = replay_path if replay_path is not None else (existing["replay_path"] if existing else None)
        parsed_json = json.dumps(parsed) if parsed is not None else (existing["parsed_json"] if existing else None)
        conn.execute(
            """INSERT INTO matches (match_id, scraped_json, replay_path, parsed_json, parse_error)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(match_id) DO UPDATE SET
                   scraped_json = excluded.scraped_json,
                   replay_path = excluded.replay_path,
                   parsed_json = excluded.parsed_json,
                   parse_error = excluded.parse_error""",
            (match_id, scraped_json, replay_path, parsed_json, parse_error),
        )
        conn.commit()


def get_match(match_id):
    with _lock, _connect() as conn:
        row = conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        return _row_to_match(row) if row else None


def list_matches(limit=20):
    with _lock, _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM matches ORDER BY match_id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [_row_to_match(r) for r in rows]


def _row_to_match(row):
    return {
        "match_id": row["match_id"],
        "scraped": json.loads(row["scraped_json"]) if row["scraped_json"] else None,
        "replay_path": row["replay_path"],
        "parsed": json.loads(row["parsed_json"]) if row["parsed_json"] else None,
        "parse_error": row["parse_error"],
    }


def replay_path_for(match_id):
    return os.path.join(REPLAY_DIR, f"{match_id}.aoe2record")
