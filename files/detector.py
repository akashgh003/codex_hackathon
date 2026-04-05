"""
detector.py — Step 2 of the agent loop.
Runs rule-based data quality checks on the profile from profiler.py.

Checks performed:
  1. NULL RATE       — column null_rate > threshold
  2. TYPE DRIFT      — dtype changed vs baseline (stored in SQLite)
  3. DUPLICATES      — duplicate row count exceeds threshold
  4. RANGE VIOLATION — numeric values outside [min_baseline * factor, max_baseline * factor]
  5. ROW DROP        — row count dropped > threshold vs last run

Each anomaly is a dict:
  {column, check_type, severity, detail, current_value, expected_value}
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "data/audit.db")

# Thresholds (tune these for your dataset)
NULL_RATE_THRESHOLD = 0.10          # >10% nulls → anomaly
DUPE_RATE_THRESHOLD = 0.05          # >5% duplicate rows → anomaly
ROW_DROP_THRESHOLD = 0.20           # >20% fewer rows than last run → anomaly
RANGE_FACTOR = 2.0                  # value outside 2x historical max/min → anomaly


def _get_baseline(conn: sqlite3.Connection) -> dict | None:
    """Retrieve last known good profile from SQLite baselines table."""
    try:
        cursor = conn.execute(
            "SELECT profile_json FROM baselines ORDER BY recorded_at DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if row:
            import json
            return json.loads(row[0])
    except Exception:
        pass
    return None


def _save_baseline(conn: sqlite3.Connection, profile: dict):
    """Persist the current profile as the new baseline."""
    import json
    conn.execute(
        "INSERT INTO baselines (profile_json, recorded_at) VALUES (?, ?)",
        (json.dumps(profile), datetime.utcnow().isoformat())
    )
    conn.commit()


def run_checks(profile: dict, df: pd.DataFrame, conn: sqlite3.Connection) -> list[dict]:
    """Run all DQ checks. Returns list of anomaly dicts."""
    anomalies = []
    baseline = _get_baseline(conn)

    # ── 1. NULL RATE ────────────────────────────────────────────────────────
    for col, stats in profile["columns"].items():
        if stats["null_rate"] > NULL_RATE_THRESHOLD:
            anomalies.append({
                "column": col,
                "check_type": "null_rate",
                "severity": "HIGH" if stats["null_rate"] > 0.3 else "MEDIUM",
                "detail": f"Null rate {stats['null_rate']:.1%} exceeds threshold {NULL_RATE_THRESHOLD:.0%}",
                "current_value": stats["null_rate"],
                "expected_value": f"<= {NULL_RATE_THRESHOLD}",
            })

    # ── 2. TYPE DRIFT ────────────────────────────────────────────────────────
    if baseline:
        for col, stats in profile["columns"].items():
            if col in baseline.get("columns", {}):
                prev_dtype = baseline["columns"][col]["dtype"]
                curr_dtype = stats["dtype"]
                if prev_dtype != curr_dtype:
                    anomalies.append({
                        "column": col,
                        "check_type": "type_drift",
                        "severity": "HIGH",
                        "detail": f"dtype changed from '{prev_dtype}' to '{curr_dtype}'",
                        "current_value": curr_dtype,
                        "expected_value": prev_dtype,
                    })

    # ── 3. DUPLICATES ────────────────────────────────────────────────────────
    dupe_count = int(df.duplicated().sum())
    dupe_rate = dupe_count / len(df) if len(df) > 0 else 0
    if dupe_rate > DUPE_RATE_THRESHOLD:
        anomalies.append({
            "column": "__all__",
            "check_type": "duplicates",
            "severity": "MEDIUM",
            "detail": f"{dupe_count} duplicate rows ({dupe_rate:.1%} of dataset)",
            "current_value": dupe_rate,
            "expected_value": f"<= {DUPE_RATE_THRESHOLD}",
        })

    # ── 4. RANGE VIOLATION ───────────────────────────────────────────────────
    if baseline:
        for col, stats in profile["columns"].items():
            bstats = baseline.get("columns", {}).get(col, {})
            if (
                stats["min"] is not None
                and bstats.get("min") is not None
                and bstats.get("max") is not None
            ):
                b_min = bstats["min"]
                b_max = bstats["max"]
                c_min = stats["min"]
                c_max = stats["max"]
                # Allow RANGE_FACTOR headroom
                allowed_min = b_min * RANGE_FACTOR if b_min < 0 else b_min / RANGE_FACTOR
                allowed_max = b_max * RANGE_FACTOR

                if c_min < allowed_min or c_max > allowed_max:
                    anomalies.append({
                        "column": col,
                        "check_type": "range_violation",
                        "severity": "HIGH",
                        "detail": f"Range [{c_min}, {c_max}] outside expected [{allowed_min:.2f}, {allowed_max:.2f}]",
                        "current_value": f"[{c_min}, {c_max}]",
                        "expected_value": f"[{allowed_min:.2f}, {allowed_max:.2f}]",
                    })

    # ── 5. ROW DROP ──────────────────────────────────────────────────────────
    if baseline:
        prev_rows = baseline.get("row_count", 0)
        curr_rows = profile["row_count"]
        if prev_rows > 0:
            drop_rate = (prev_rows - curr_rows) / prev_rows
            if drop_rate > ROW_DROP_THRESHOLD:
                anomalies.append({
                    "column": "__all__",
                    "check_type": "row_drop",
                    "severity": "HIGH",
                    "detail": f"Row count dropped from {prev_rows} to {curr_rows} ({drop_rate:.1%} loss)",
                    "current_value": curr_rows,
                    "expected_value": f">= {int(prev_rows * (1 - ROW_DROP_THRESHOLD))}",
                })

    # Always save current profile as new baseline after checks
    _save_baseline(conn, profile)

    return anomalies


if __name__ == "__main__":
    from profiler import load_and_profile
    import db

    profile, df = load_and_profile()
    conn = db.get_connection()
    anomalies = run_checks(profile, df, conn)
    print(f"Anomalies found: {len(anomalies)}")
    for a in anomalies:
        print(f"  [{a['severity']}] {a['check_type']} on '{a['column']}': {a['detail']}")
