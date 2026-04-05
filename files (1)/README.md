# 🔬 DQ Self-Healing Agent

> "Every other team is watching if the pipeline ran. We're watching if the data inside is trustworthy — and fixing it automatically."

## Architecture

```
main.py (agent loop, 30s poll)
  ├── profiler.py       → nulls, types, ranges, row count
  ├── detector.py       → 5 rule-based DQ checks
  ├── healer.py         → backup → fix → verify → retry (3x) → audit
  │     └── codex_client.py  ← ONLY Claude API call in the system
  └── db.py             → SQLite: audit_log, baselines, run_log

app.py (Streamlit dashboard, 5s refresh)
inject_bad.py (demo: inject nulls / type drift / dupes / range violations)
seed_data.py  (generate starter CSV)
```

## 6-Hour Build Order

| Hour | Task |
|------|------|
| 0:00 | `pip install -r requirements.txt` + `python seed_data.py` |
| 0:15 | Test profiler: `python profiler.py` |
| 0:30 | Test detector: `python detector.py` |
| 0:45 | Test codex_client: `python codex_client.py` |
| 1:00 | Test healer end-to-end |
| 1:30 | Run `python main.py` in one terminal |
| 2:00 | Run `streamlit run app.py` in second terminal |
| 2:30 | Run `python inject_bad.py` — watch it heal live |
| 3:00 | Polish dashboard, add demo scenarios |
| 4:00 | Full dress rehearsal: inject all 4 scenarios |
| 5:00 | Buffer / bug fixes |
| 5:30 | Prepare demo script (inject → watch heal → show audit trail) |

## Quick Start

```bash
# Install deps
pip install -r requirements.txt

# Seed demo data
python seed_data.py

# Terminal 1: Start the agent
python main.py

# Terminal 2: Start the dashboard
streamlit run app.py

# Terminal 3: Inject bad data (any time)
python inject_bad.py --scenario null    # 40% nulls
python inject_bad.py --scenario type    # type drift
python inject_bad.py --scenario dupe    # duplicates
python inject_bad.py --scenario range   # extreme outliers
python inject_bad.py                    # random scenario
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_PATH` | `data/sales.csv` | Path to monitored CSV |
| `DB_PATH` | `data/audit.db` | SQLite database path |
| `POLL_INTERVAL` | `30` | Agent poll interval (seconds) |

## The Differentiators

1. **Self-healing retry loop** — tries a fix, verifies it, tries a *different* approach if it failed (up to 3x)
2. **Full audit trail** — every anomaly logged with diagnosis, fix code, outcome, attempts
3. **UNRESOLVABLE path** — restores backup + writes human-readable recommendation
4. **$0.20 budget** — only ~20 Claude calls across a 6-hour demo
5. **Column trust scores** — 0-100 score per column, live on dashboard
