"""
main.py — Agent main loop.

Runs forever:
  1. Load + profile CSV
  2. Run DQ checks (detector)
  3. If anomalies found → heal each one (healer → codex_client)
  4. Log run summary to SQLite
  5. Sleep 30s → repeat

Usage:
  python main.py
  DATA_PATH=data/sales.csv DB_PATH=data/audit.db python main.py
"""

import time
import os
import logging
from datetime import datetime

import db
from profiler import load_and_profile
from detector import run_checks
from healer import heal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_PATH = os.environ.get("DATA_PATH", "data/sales.csv")
DB_PATH = os.environ.get("DB_PATH", "data/audit.db")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))


def run_once(conn) -> dict:
    """Single agent cycle. Returns run summary dict."""
    start = datetime.utcnow()
    codex_calls = 0
    healed = 0
    unresolvable = 0

    # ── Step 1: Profile ──────────────────────────────────────────────────────
    log.info(f"[STEP 1] Loading + profiling: {DATA_PATH}")
    try:
        profile, df = load_and_profile(DATA_PATH)
        log.info(f"  → {profile['row_count']} rows, {len(profile['columns'])} columns")
    except Exception as e:
        log.error(f"  ✗ Failed to load data: {e}")
        return {}

    # ── Step 2: Detect ───────────────────────────────────────────────────────
    log.info("[STEP 2] Running quality checks…")
    anomalies = run_checks(profile, df, conn)
    log.info(f"  → {len(anomalies)} anomaly(ies) found")

    if not anomalies:
        log.info("  ✓ Data is clean. Sleeping.")
        _log_run(conn, profile["row_count"], 0, 0, 0, 0)
        return {"status": "clean", "anomalies": 0}

    # ── Step 3: Heal each anomaly ────────────────────────────────────────────
    for i, anomaly in enumerate(anomalies, 1):
        log.info(
            f"[STEP 3] Healing anomaly {i}/{len(anomalies)}: "
            f"[{anomaly['severity']}] {anomaly['check_type']} on '{anomaly['column']}'"
        )
        codex_calls += 1  # at least 1 call per anomaly (may retry)

        df, outcome = heal(
            anomaly=anomaly,
            df=df,
            profile=profile,
            conn=conn,
            data_path=DATA_PATH,
        )

        if outcome == "HEALED":
            healed += 1
            log.info(f"  ✓ HEALED: {anomaly['column']} / {anomaly['check_type']}")
        else:
            unresolvable += 1
            log.warning(f"  ✗ UNRESOLVABLE: {anomaly['column']} / {anomaly['check_type']}")

    # ── Step 4: Log run summary ──────────────────────────────────────────────
    _log_run(conn, profile["row_count"], len(anomalies), healed, unresolvable, codex_calls)

    summary = {
        "status": "ran",
        "anomalies": len(anomalies),
        "healed": healed,
        "unresolvable": unresolvable,
        "codex_calls": codex_calls,
        "duration_s": (datetime.utcnow() - start).total_seconds(),
    }
    log.info(f"[DONE] {summary}")
    return summary


def _log_run(conn, row_count, anomaly_count, healed, unresolvable, codex_calls):
    conn.execute(
        """INSERT INTO run_log
           (timestamp, row_count, anomaly_count, healed_count, unresolvable, codex_calls)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (datetime.utcnow().isoformat(), row_count, anomaly_count, healed, unresolvable, codex_calls),
    )
    conn.commit()


def main():
    log.info("=" * 50)
    log.info("  Data Quality Self-Healing Agent — STARTING")
    log.info(f"  Dataset : {DATA_PATH}")
    log.info(f"  Database: {DB_PATH}")
    log.info(f"  Poll    : every {POLL_INTERVAL}s")
    log.info("=" * 50)

    conn = db.get_connection(DB_PATH)

    while True:
        try:
            run_once(conn)
        except KeyboardInterrupt:
            log.info("Agent stopped by user.")
            break
        except Exception as e:
            log.error(f"Unexpected error in agent loop: {e}", exc_info=True)

        log.info(f"  Sleeping {POLL_INTERVAL}s…\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
