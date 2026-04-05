"""
db.py — SQLite initialization.
Creates audit_log and baselines tables if they don't exist.
"""

import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", "data/audit.db")


def get_connection(path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    _init_schema(conn)
    return conn


def _init_schema(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT NOT NULL,
            column_name      TEXT NOT NULL,
            check_type       TEXT NOT NULL,
            severity         TEXT NOT NULL,
            anomaly_detail   TEXT,
            outcome          TEXT NOT NULL,   -- HEALED or UNRESOLVABLE
            attempts         INTEGER,
            diagnosis        TEXT,
            fix_code         TEXT,
            recommended_action TEXT
        );

        CREATE TABLE IF NOT EXISTS baselines (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_json TEXT NOT NULL,
            recorded_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS run_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp     TEXT NOT NULL,
            row_count     INTEGER,
            anomaly_count INTEGER,
            healed_count  INTEGER,
            unresolvable  INTEGER,
            codex_calls   INTEGER
        );
    """)
    conn.commit()
    try:
        conn.execute("ALTER TABLE audit_log ADD COLUMN file_name TEXT;")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE audit_log ADD COLUMN affected_rows INTEGER;")
    except sqlite3.OperationalError:
        pass
