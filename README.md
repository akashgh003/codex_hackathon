# 🛡️ DQ Sentinel — Autonomous Data Quality Self-Healing Agent

> *"Every other monitoring tool tells you the pipeline ran. We tell you whether the data inside it can be trusted — and we fix it before you even wake up."*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.35+-FF4B4B?style=flat&logo=streamlit&logoColor=white)](https://streamlit.io)
[![SQLite](https://img.shields.io/badge/SQLite-3-003B57?style=flat&logo=sqlite&logoColor=white)](https://sqlite.org)


---

## What is DQ Sentinel?

DQ Sentinel is an **autonomous data quality agent** that watches your data pipeline 24/7, detects anomalies the moment they appear, and heals them automatically — without waking up a data engineer.

Most data quality tools alert you when something breaks. DQ Sentinel tries to fix it. It backs up your data, asks Claude to generate a targeted repair, applies it, verifies the fix worked — and if it didn't, tries a completely different approach. Up to 3 times. Only when all 3 fail does it escalate to a human, with a full audit trail and a recommended action already written.

**The architecture is intentionally hybrid:** detection is pure rule-based Python (deterministic, auditable, zero hallucination risk). Claude is called only for the one thing that requires judgment — figuring out *how* to fix a novel anomaly in a specific column with specific statistics. Everything else runs at $0 cost.

---

## The Problem It Solves

Data breaks silently. Nobody gets an alert that says "40% of your price column just became null." It quietly rots while dashboards refresh, pipelines keep running, and decisions are made on corrupted inputs.

| How data breaks | Real consequence |
|---|---|
| **40% of prices become null** | Revenue dashboard shows ₹0. Finance team makes wrong decisions for 3 days. |
| **Age column drifts from int to string** | Fraud detection model crashes. Transactions go unscreened. |
| **200 duplicate orders appear** | Customer billed three times. Support tickets flood in. |
| **Salary column shows 100× normal values** | Payroll approves a ₹99,99,99,999 transfer. |
| **10,000 rows silently become 3,000** | Entire record batch lost. Analytics wrong by 70%. |

> Bad data costs companies **$12.9 million per year on average** — Gartner

DQ Sentinel catches all five of these failure classes. Automatically.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  main.py  (Agent Loop)                      │
│                  ↻  polls every 30 seconds                  │
├──────────────┬───────────────┬──────────────┬──────────────┤
│ profiler.py  │  detector.py  │  healer.py   │   db.py      │
│              │               │              │              │
│ Compute      │  5 rule-based │ Backup →     │ SQLite:      │
│ nulls,       │  DQ checks    │ Fix →        │ audit_log    │
│ types,       │  (no LLM)     │ Verify →     │ baselines    │
│ ranges,      │               │ Retry (3×)   │ run_log      │
│ row count    │               │              │              │
└──────────────┴───────────────┴──────┬───────┴──────────────┘
                                      │  only fires on anomaly
                               ┌──────▼──────┐
                               │codex_client │
                               │    .py      │
                               │ Claude API  │
                               │  (1 call)   │
                               └─────────────┘

app.py  (Streamlit Dashboard — 5s auto-refresh from SQLite)
├── Sidebar : Agent deploy controls + test utilities
└── Main    : Live metrics · Trust scores · Event feed · Audit log
```

### The Three Layers of Intelligence

| Layer | File | Technology | Cost |
|---|---|---|---|
| **Profile** | `profiler.py` | Pure Python + Pandas | $0 |
| **Detect** | `detector.py` | Rule-based (5 checks) | $0 |
| **Heal** | `healer.py` + `codex_client.py` | Python + Claude API | ~$0.003/call |
| **Observe** | `app.py` | Streamlit + SQLite | $0 |

Only **one function** calls the LLM. In a full 6-hour demo with 4 injected scenarios, total Claude spend is under ₹20.

---

## Flowcharts

### 1 — Agent Main Loop

```
                    ┌─────────────┐
                    │ Agent starts│
                    └──────┬──────┘
                           │
                           ▼
              ┌────────────────────────────┐
              │  Step 1 — Load + profile   │
              │  Compute nulls, types,     │
              │  ranges, row count         │
              └────────────┬───────────────┘
                           │
                           ▼
              ┌────────────────────────────┐
              │  Step 2 — Run quality      │
              │  checks                    │
              │  Null rate · Type drift    │
              │  Dupes · Range · Row drop  │
              └────────────┬───────────────┘
                           │
                    ┌──────▼──────┐
                    │  Anomalies  │
                    │  found?     │
                    └──────┬──────┘
               No ◄────────┤────────► Yes
               │           │                │
               ▼           │                ▼
          Sleep 30s         │   ┌────────────────────────┐
          → repeat          │   │  Step 3/4              │
                            │   │  Self-heal each anomaly│
                            │   │  (healer.py)           │
                            │   └────────────┬───────────┘
                            │                │
                            │   ┌────────────▼───────────┐
                            │   │  Step 5 — Log result   │
                            │   │  HEALED or             │
                            │   │  UNRESOLVABLE          │
                            │   └────────────┬───────────┘
                            │                │
                            └────────────────┘
                                  Sleep 30s → repeat
```

---

### 2 — Self-Healing Engine

```
            ┌──────────────────────────────────┐
            │    Anomaly detected by detector  │
            └──────────────────┬───────────────┘
                               │
                               ▼
            ┌──────────────────────────────────┐
            │  Backup original data            │
            │  Snapshot saved before any change│
            └──────────────────┬───────────────┘
                               │
                               ▼
                        attempt = 1
                               │
                               ▼
            ┌──────────────────────────────────┐
            │  Call Claude API                 │
            │  Send: column, anomaly type,     │
            │  stats, sample values,           │
            │  previous failed fix (if retry)  │
            │                                  │
            │  Receive:                        │
            │  • diagnosis                     │
            │  • pandas fix_code               │
            │  • confidence                    │
            │  • recommended_action            │
            └──────────────────┬───────────────┘
                               │
                               ▼
            ┌──────────────────────────────────┐
            │  Apply fix to DataFrame          │
            │  Execute in sandboxed scope      │
            └──────────────────┬───────────────┘
                               │
                               ▼
            ┌──────────────────────────────────┐
            │  Re-run the same DQ check        │
            │  Did the anomaly clear?          │
            └─────────┬────────────────────────┘
                      │
              ┌───────▼────────┐
              │    Pass?       │
              └───────┬────────┘
                      │
        YES ──────────┼────────── NO
         │            │                │
         ▼            │         attempt < 3?
      HEALED          │          │         │
      Save CSV        │         YES        NO
      Log audit       │          │         │
                      │     attempt++      ▼
                      │     retry with  UNRESOLVABLE
                      │     new prompt  Restore backup
                      │     ◄──────────  Log recommendation
                      │
                      ▼
         ┌────────────────────────────┐
         │  Write full audit record   │
         │  to SQLite                 │
         └────────────────────────────┘
```

**The retry loop is the key differentiator.** On attempt 2+, the failed fix is included in the prompt with the instruction "do NOT repeat this approach — try a fundamentally different strategy." Claude generates a completely different transformation each time.

---

### 3 — Frontend Dashboard Flow

```
            ┌──────────────────────────────────┐
            │      User opens browser          │
            └──────────────────┬───────────────┘
                               │
                               ▼
            ┌──────────────────────────────────┐
            │  Streamlit app starts (app.py)   │
            │  Reads SQLite every 5s           │
            │  via st.rerun()                  │
            └──────┬─────────────────┬─────────┘
                   │                 │
         ┌─────────▼──────┐  ┌───────▼──────────────┐
         │    SIDEBAR     │  │      MAIN PAGE        │
         │                │  │                       │
         │ ● Agent Status │  │  Header metrics bar   │
         │                │  │  Rows · Checks ·      │
         │ 🚀 Deploy      │  │  Healed% · Claude     │
         │ ▶ Start Agent  │  │  calls · Uptime       │
         │ ⏹ Stop Agent   │  │                       │
         │                │  │  Column trust scores  │
         │ 🧪 Test        │  │  Green/amber/red bar  │
         │ Generate Data  │  │  per column (0–100)   │
         │                │  │                       │
         │ Inject Bad:    │  │  Live event feed      │
         │ 🔴 High Nulls  │  │  Last 14 events       │
         │ 🟠 Type Drift  │  │  color-coded by       │
         │ 🟡 Duplicates  │  │  outcome              │
         │ 🔵 Range Viol. │  │                       │
         │                │  │  Anomaly detail panel │
         │ 🔍 Check Now   │  │  Expandable cards:    │
         │ 🗑️ Clear Logs  │  │  diagnosis + fix code │
         │                │  │  + human action       │
         │ ↻ every 5s     │  │                       │
         └────────────────┘  │  Full audit log table │
                             │  Time · Column · Fix  │
                             │  · Outcome            │
                             └───────────┬───────────┘
                                         │
                               Auto-refresh 5s → loop
```

---

## The 5 Data Quality Checks

```
detector.py — all checks are deterministic, no LLM involved

┌──────────────────┬──────────────────────────────┬───────────┬──────────┐
│ Check            │ What it detects              │ Threshold │ Severity │
├──────────────────┼──────────────────────────────┼───────────┼──────────┤
│ Null Rate        │ Column has too many missing  │ > 10%     │ HIGH if  │
│                  │ values                       │ nulls     │ > 30%    │
├──────────────────┼──────────────────────────────┼───────────┼──────────┤
│ Type Drift       │ Column dtype changed vs      │ Any       │ HIGH     │
│                  │ last baseline                │ change    │          │
├──────────────────┼──────────────────────────────┼───────────┼──────────┤
│ Duplicates       │ Duplicate rows in dataset    │ > 5%      │ MEDIUM   │
│                  │                              │ of rows   │          │
├──────────────────┼──────────────────────────────┼───────────┼──────────┤
│ Range Violation  │ Values outside historical    │ > 2×      │ HIGH     │
│                  │ min/max bounds               │ hist. max │          │
├──────────────────┼──────────────────────────────┼───────────┼──────────┤
│ Row Drop         │ Dataset shrank significantly │ > 20%     │ HIGH     │
│                  │ vs last run                  │ fewer rows│          │
└──────────────────┴──────────────────────────────┴───────────┴──────────┘
```

---

## File Structure

```
dq-sentinel/
├── main.py           # Agent loop — runs forever, polls every 30s
├── profiler.py       # Step 1: Load CSV, compute column statistics
├── detector.py       # Step 2: Run 5 rule-based DQ checks
├── healer.py         # Step 3/4: Self-healing engine with retry loop
├── codex_client.py   # The ONLY Claude API call — generates fix code
├── db.py             # SQLite schema init (audit_log, baselines, run_log)
├── app.py            # Streamlit dashboard with sidebar controls
├── inject_bad.py     # Demo: inject null/type/dupe/range corruption
├── seed_data.py      # Generate realistic 1,000-row sales CSV
├── requirements.txt
├── data/
│   ├── sales.csv     # Monitored dataset (created by seed_data.py)
│   └── audit.db      # SQLite database (created on first run)
└── README.md
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Seed demo data

```bash
python seed_data.py
# Creates data/sales.csv — 1,000 rows
# Columns: order_id, product, price, quantity, revenue, region, customer_age, discount
```

### 3. Start the agent

```bash
python main.py
# Polls data/sales.csv every 30s
# Writes all events to data/audit.db
```

### 4. Launch the dashboard

```bash
streamlit run app.py
# Opens at http://localhost:8501
# Auto-refreshes every 5s
```

### 5. Inject bad data and watch it heal

```bash
python inject_bad.py --scenario null    # 40% of a numeric column → NaN
python inject_bad.py --scenario type    # 30% of a numeric column → "CORRUPT_STRING"
python inject_bad.py --scenario dupe    # +200 duplicate rows injected
python inject_bad.py --scenario range   # 5% of rows set to 100× normal value
python inject_bad.py                    # random scenario
```

The agent detects and heals in the next 30-second poll cycle. Watch the dashboard update live.

Or use the **Inject Bad Data** buttons directly from the dashboard sidebar.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATA_PATH` | `data/sales.csv` | Path to the monitored CSV |
| `DB_PATH` | `data/audit.db` | SQLite database path |
| `POLL_INTERVAL` | `30` | Agent polling interval in seconds |

---

## Requirements

```
streamlit>=1.35.0
pandas>=2.0.0
numpy>=1.24.0
requests>=2.31.0
```

Python 3.11+ recommended. No external database required — SQLite is the only dependency.

---

## Cost Model

| Operation | Frequency | Cost |
|---|---|---|
| Profile + detect | Every 30s | $0 — pure Python |
| Verify fix | After each attempt | $0 — rule-based |
| Dashboard refresh | Every 5s | $0 — SQLite reads |
| Claude fix generation | Only when anomaly fires | ~$0.003 per call |
| **Full 6-hour demo, 4 scenarios** | — | **< $0.20 total** |

Claude is invoked **only when a rule fires.** Quiet periods cost exactly $0.

---

## Audit Trail

Every healing event is persisted to SQLite:

```sql
SELECT timestamp, column_name, check_type, severity,
       outcome, attempts, diagnosis, fix_code, recommended_action
FROM audit_log
ORDER BY timestamp DESC;
```

| Field | Example value |
|---|---|
| `timestamp` | `2025-04-05 03:42:17` |
| `column_name` | `price` |
| `check_type` | `null_rate` |
| `severity` | `HIGH` |
| `outcome` | `HEALED` |
| `attempts` | `2` |
| `diagnosis` | `Price nulls caused by upstream ETL join failure on product_id` |
| `fix_code` | `df['price'] = df['price'].fillna(df['price'].median())` |
| `recommended_action` | `Verify ETL join on product_id. Check source table completeness.` |

When a stakeholder asks "what happened to revenue data on Tuesday at 3 AM?" — the answer is in this table, with the exact fix code that ran.

---

## Roadmap

- [ ] PostgreSQL / BigQuery / Snowflake connector
- [ ] Multi-table monitoring (one agent instance per table)
- [ ] Slack / PagerDuty alerts for UNRESOLVABLE events
- [ ] Airflow / Prefect integration as a pipeline sidecar
- [ ] Column-level anomaly trend charts in dashboard
- [ ] Custom threshold configuration per column via UI
- [ ] Email digest of nightly healing summary
- [ ] Schema drift detection (new/removed columns)

---

## How It Differs from Existing Tools

| Existing tools | DQ Sentinel |
|---|---|
| Alert when something breaks | Fix it before the alert fires |
| One-shot fix attempt | 3 attempts, different strategy each time |
| Opaque AI decisions | Deterministic detection + explainable LLM fix |
| Alert fatigue | Full audit trail with context |
| Expensive continuous LLM inference | LLM called only when a rule fires |
| Requires data engineer at 2 AM | Agent handles it, escalates only when truly stuck |

---


## Built At

Built during a hackathon to demonstrate that autonomous AI agents can make data infrastructure genuinely self-healing — not just observable.

The detection layer is intentionally rule-based and deterministic. The LLM layer is intentionally thin — invoked only when human-level judgment is needed to generate a fix for a novel anomaly in a specific column with specific statistics. That is the right way to use AI in a production data system.
