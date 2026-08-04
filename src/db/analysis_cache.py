from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional


try:
    from overstats.src.runtime_paths import ensure_parent, runtime_path
except ModuleNotFoundError:
    from src.runtime_paths import ensure_parent, runtime_path


ANALYSIS_CACHE_DB_PATH = runtime_path("db", "analysis_cache.sqlite3")
_LOCK = threading.RLock()


def _connect() -> sqlite3.Connection:
    ensure_parent(ANALYSIS_CACHE_DB_PATH)
    connection = sqlite3.connect(str(ANALYSIS_CACHE_DB_PATH), timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def initialize() -> None:
    with _LOCK, _connect() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS match_ai_analysis_cache (
                namespace TEXT NOT NULL,
                analysis_version TEXT NOT NULL,
                match_id TEXT NOT NULL,
                match_kind TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                provider_model TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(namespace, analysis_version, match_id)
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_match_ai_analysis_expires ON match_ai_analysis_cache(expires_at)")
        connection.commit()


def get(namespace: str, version: str, match_id: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    initialize()
    with _LOCK, _connect() as connection:
        row = connection.execute(
            "SELECT * FROM match_ai_analysis_cache WHERE namespace = ? AND analysis_version = ? AND match_id = ? AND expires_at > ?",
            (namespace, version, match_id, now),
        ).fetchone()
        connection.execute("DELETE FROM match_ai_analysis_cache WHERE expires_at <= ?", (now,))
        connection.commit()
    if not row:
        return None
    try:
        analysis = json.loads(str(row["analysis_json"]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(analysis, dict):
        return None
    return {
        "analysis": analysis,
        "match_kind": str(row["match_kind"] or ""),
        "created_at": float(row["created_at"]),
        "expires_at": float(row["expires_at"]),
        "provider_model": str(row["provider_model"] or ""),
    }


def put(
    namespace: str,
    version: str,
    match_id: str,
    match_kind: str,
    analysis: Dict[str, Any],
    provider_model: str = "",
    ttl_seconds: int = 86400,
) -> Dict[str, float]:
    created_at = time.time()
    expires_at = created_at + max(1, int(ttl_seconds))
    encoded = json.dumps(analysis, ensure_ascii=False, separators=(",", ":"))
    initialize()
    with _LOCK, _connect() as connection:
        connection.execute(
            """
            INSERT INTO match_ai_analysis_cache
                (namespace, analysis_version, match_id, match_kind, analysis_json, created_at, expires_at, provider_model)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(namespace, analysis_version, match_id) DO UPDATE SET
                match_kind = excluded.match_kind,
                analysis_json = excluded.analysis_json,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at,
                provider_model = excluded.provider_model
            """,
            (namespace, version, match_id, match_kind, encoded, created_at, expires_at, provider_model),
        )
        connection.commit()
    return {"created_at": created_at, "expires_at": expires_at}
