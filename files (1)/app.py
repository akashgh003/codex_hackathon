from __future__ import annotations

import json
import math
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from html import escape
from pathlib import Path
import base64
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
import streamlit.components.v1 as components

import db
from main import run_once

DB_PATH = os.environ.get("DB_PATH", "data/audit.db")
DATA_PATH = os.environ.get("DATA_PATH", "data/sales.csv")
TARGET_DATA_PATH = os.environ.get("TARGET_DATA_PATH", "data/target.csv")
REFRESH_INTERVAL = 5

st.set_page_config(page_title="DQ Self-Healing Agent", page_icon="DQ", layout="wide")


def inject_styles() -> None:
    st.markdown(
        """
        <style>
            :root {
                --bg: #F8FAFC;
                --bg-accent: #E2E8F0;
                --surface: rgba(255, 255, 255, 0.65);
                --surface-soft: rgba(241, 245, 249, 0.45);
                --text: #0F172A;
                --text-soft: #475569;
                --muted: #64748B;
                --border: rgba(148, 163, 184, 0.4);
                --shadow: 0 10px 40px rgba(30, 41, 59, 0.08);
                --ok: #10B981;
                --warn: #F59E0B;
                --bad: #EF4444;
                --cyan: #0284C7;
                --purple: #6D28D9;
                --glow: 0 0 20px rgba(2, 132, 199, 0.15);
            }
            @media (prefers-color-scheme: dark) {
                :root {
                    --bg: #0B1120;
                    --bg-accent: #020617;
                    --surface: rgba(30, 41, 59, 0.45);
                    --surface-soft: rgba(51, 65, 85, 0.3);
                    --text: #F8FAFC;
                    --text-soft: #94A3B8;
                    --muted: #64748B;
                    --border: rgba(56, 189, 248, 0.25);
                    --shadow: 0 10px 40px rgba(2, 6, 23, 0.9);
                    --ok: #10B981;
                    --warn: #F59E0B;
                    --bad: #EF4444;
                    --cyan: #38BDF8;
                    --purple: #C084FC;
                    --glow: 0 0 20px rgba(56, 189, 248, 0.35);
                }
            }
            html[data-theme="dark"], body[data-theme="dark"] {
                --bg: #0B1120;
                --bg-accent: #020617;
                --surface: rgba(30, 41, 59, 0.45);
                --surface-soft: rgba(51, 65, 85, 0.3);
                --text: #F8FAFC;
                --text-soft: #94A3B8;
                --muted: #64748B;
                --border: rgba(56, 189, 248, 0.25);
                --shadow: 0 10px 40px rgba(2, 6, 23, 0.9);
                --ok: #10B981;
                --warn: #F59E0B;
                --bad: #EF4444;
                --cyan: #38BDF8;
                --purple: #C084FC;
                --glow: 0 0 20px rgba(56, 189, 248, 0.35);
            }
            html[data-theme="light"], body[data-theme="light"] {
                --bg: #F8FAFC;
                --bg-accent: #E2E8F0;
                --surface: rgba(255, 255, 255, 0.65);
                --surface-soft: rgba(241, 245, 249, 0.45);
                --text: #0F172A;
                --text-soft: #475569;
                --muted: #64748B;
                --border: rgba(148, 163, 184, 0.4);
                --shadow: 0 10px 40px rgba(30, 41, 59, 0.08);
                --ok: #10B981;
                --warn: #F59E0B;
                --bad: #EF4444;
                --cyan: #0284C7;
                --purple: #6D28D9;
                --glow: 0 0 20px rgba(2, 132, 199, 0.15);
            }
            .stApp {
                background-image: radial-gradient(circle at 10% 20%, var(--bg-accent) 0%, var(--bg) 90%);
                color: var(--text);
            }
            [data-testid="stAppViewContainer"],
            [data-testid="stHeader"],
            [data-testid="stToolbar"] {
                background: transparent !important;
            }
            [data-testid="block-container"] {
                background: transparent !important;
            }
            p, span, label, h1, h2, h3, h4, h5, h6, small, div {
                color: var(--text) !important;
            }
            
            /* -- Hackathon Glassmorphic Card Styles -- */
            .kpi-card {
                background: var(--surface);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 24px;
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                box-shadow: var(--shadow);
                transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1), box-shadow 0.3s ease, border-color 0.3s ease;
                margin-bottom: 20px;
                position: relative;
                overflow: hidden;
            }
            .kpi-card:hover {
                transform: translateY(-8px) scale(1.02);
                box-shadow: var(--glow), var(--shadow);
                border-color: var(--cyan);
            }
            .kpi-card::before {
                content: '';
                position: absolute;
                top: 0; left: 0; right: 0;
                height: 4px;
                background: linear-gradient(90deg, var(--cyan), var(--purple));
                opacity: 0;
                transition: opacity 0.3s ease;
            }
            .kpi-card:hover::before {
                opacity: 1;
            }
            .kpi-value {
                font-size: 42px;
                font-weight: 900;
                background: linear-gradient(135deg, var(--text) 0%, var(--cyan) 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin: 5px 0;
                filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1));
            }
            .kpi-title {
                font-size: 13px;
                text-transform: uppercase;
                letter-spacing: 2px;
                color: var(--text-soft) !important;
                font-weight: 700;
            }
            
            .hero {
                border: 1px solid var(--border);
                border-radius: 20px;
                padding: 32px;
                background: linear-gradient(120deg, var(--surface) 0%, var(--surface-soft) 100%);
                backdrop-filter: blur(20px);
                box-shadow: var(--shadow);
                margin-bottom: 24px;
                position: relative;
                overflow: hidden;
            }
            .hero::after {
                content: '';
                position: absolute;
                top: -50%; left: -50%; width: 200%; height: 200%;
                background: radial-gradient(circle, rgba(56, 189, 248, 0.05) 0%, transparent 70%);
                z-index: 0;
            }
            .hero * { z-index: 1; position: relative; }
            .hero h1 {
                margin: 0;
                color: var(--text) !important;
                font-size: 48px;
                font-weight: 900;
                letter-spacing: -1px;
                background: linear-gradient(90deg, var(--text), var(--cyan));
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .hero p {
                margin: 10px 0 0 0;
                color: var(--muted);
                font-size: 18px;
            }
            .panel {
                border: 1px solid var(--border);
                border-radius: 12px;
                padding: 14px;
                background: var(--surface);
                min-height: 108px;
                animation: fadeSlideIn 0.45s ease both;
            }
            .panel h4 {
                margin: 0 0 8px 0;
                font-size: 13px;
                color: var(--cyan);
                text-transform: uppercase;
                letter-spacing: 0.4px;
            }
            .panel .value {
                font-size: 21px;
                color: var(--text);
                font-weight: 600;
            }
            .panel .meta {
                margin-top: 6px;
                color: var(--muted);
                font-size: 12px;
            }
            .section-title {
                font-size: 24px;
                color: var(--text);
                margin-top: 6px;
                margin-bottom: 8px;
                letter-spacing: -0.2px;
            }
            [data-testid="stSidebar"] {
                border-right: 1px solid var(--border);
                background: linear-gradient(180deg, var(--surface-soft) 0%, var(--surface) 100%);
            }
            [data-testid="stSidebar"] * {
                color: var(--text) !important;
            }
            [data-testid="stSidebar"] h1,
            [data-testid="stSidebar"] h2,
            [data-testid="stSidebar"] h3,
            [data-testid="stSidebar"] p,
            [data-testid="stSidebar"] label {
                color: var(--text) !important;
            }
            [data-testid="stSidebar"] [data-baseweb="radio"] > div {
                gap: 8px;
            }
            [data-testid="stSidebar"] [data-baseweb="radio"] label {
                border: 1px solid var(--border);
                border-radius: 10px;
                padding: 8px 10px;
                background: var(--surface);
                transition: all 0.2s ease;
            }
            [data-testid="stSidebar"] [data-baseweb="radio"] label:hover {
                transform: translateX(2px);
                border-color: var(--text-soft);
            }
            [data-testid="stSidebar"] [data-baseweb="radio"] label:has(input:checked) {
                background: color-mix(in srgb, var(--surface) 60%, var(--text-soft) 40%);
                border-color: var(--text-soft);
                box-shadow: inset 0 0 0 1px var(--text-soft);
            }
            [data-testid="stSidebar"] [data-baseweb="select"] > div,
            [data-testid="stSidebar"] [data-baseweb="select"] input {
                background: var(--surface) !important;
                color: var(--text) !important;
                border-color: var(--border) !important;
            }
            [data-testid="stSidebar"] [data-baseweb="select"] [role="listbox"] {
                background: var(--surface-soft) !important;
                color: var(--text) !important;
            }
            .stButton > button {
                border-radius: 10px;
                border: 1px solid var(--border);
                background: var(--surface) !important;
                color: var(--text) !important;
                transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease;
            }
            .stButton > button:hover {
                transform: translateY(-1px);
                border-color: var(--text-soft);
                box-shadow: 0 6px 16px rgba(0, 0, 0, 0.14);
            }
            .stButton > button:disabled {
                opacity: 0.45;
                color: var(--muted) !important;
            }
            [data-testid="stMetric"] {
                background: var(--surface);
                border: 1px solid var(--border);
                border-radius: 12px;
                padding: 10px 12px;
            }
            [data-testid="stMetricValue"],
            [data-testid="stMetricLabel"],
            .stMarkdown,
            .stMarkdown p,
            .stMarkdown span,
            .stDataFrame,
            .stAlert {
                color: var(--text) !important;
            }
            [data-testid="stFileUploader"] {
                background: var(--surface);
                border: 1px solid var(--border);
                border-radius: 12px;
                padding: 8px;
            }
            [data-testid="stFileUploader"] section {
                border-radius: 12px;
                border: 1px dashed var(--border);
                background: var(--surface);
            }
            [data-testid="stFileUploader"] * {
                color: var(--text) !important;
            }
            [data-testid="stDataFrameResizable"] {
                border: 1px solid var(--border);
                border-radius: 12px;
                overflow: hidden;
            }
            [data-testid="stDataFrameResizable"] div {
                color: var(--text) !important;
                background: var(--surface) !important;
            }
            [data-testid="stDataFrameResizable"] thead tr th,
            [data-testid="stDataFrameResizable"] tbody tr td {
                color: var(--text) !important;
                background: var(--surface) !important;
                border-color: var(--border) !important;
            }
            [data-testid="stAlert"] {
                background: var(--surface) !important;
                border: 1px solid var(--border) !important;
                color: var(--text) !important;
            }
            [data-testid="stRadio"] div[role="radiogroup"] label,
            [data-testid="stCheckbox"] label,
            [data-testid="stSelectbox"] label,
            [data-testid="stFileUploader"] label {
                color: var(--text) !important;
            }
            hr {
                border: none;
                height: 1px;
                background: var(--border);
            }
            .sync-table-wrap {
                max-height: 430px;
                overflow: auto;
                border: 1px solid var(--border);
                border-radius: 12px;
                background: var(--surface);
                box-shadow: var(--shadow);
            }
            .sync-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
            }
            .sync-table thead th {
                position: sticky;
                top: 0;
                z-index: 2;
                padding: 8px 10px;
                border-bottom: 1px solid var(--border);
                background: var(--surface-soft);
                text-align: left;
                white-space: nowrap;
            }
            .sync-table td {
                padding: 6px 10px;
                border-bottom: 1px solid color-mix(in srgb, var(--border) 70%, transparent);
                white-space: nowrap;
            }
            .sync-table td.match {
                background: rgba(34, 197, 94, 0.18);
                color: #14532d !important;
            }
            .sync-table td.mismatch {
                background: rgba(239, 68, 68, 0.18);
                color: #7f1d1d !important;
            }
            .plain-table-wrap {
                max-height: 380px;
                overflow: auto;
                border: 1px solid var(--border);
                border-radius: 12px;
                background: var(--surface);
            }
            .plain-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
            }
            .plain-table thead th {
                position: sticky;
                top: 0;
                z-index: 2;
                padding: 8px 10px;
                border-bottom: 1px solid var(--border);
                background: var(--surface-soft);
                text-align: left;
                white-space: nowrap;
            }
            .plain-table td {
                padding: 6px 10px;
                border-bottom: 1px solid color-mix(in srgb, var(--border) 70%, transparent);
                color: var(--text) !important;
                white-space: nowrap;
            }
            @keyframes fadeSlideIn {
                from {
                    opacity: 0;
                    transform: translateY(10px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    components.html(
        """
        <script>
            function updateTheme() {
                var parentDoc = window.parent.document;
                var appNode = parentDoc.querySelector('.stApp') || parentDoc.querySelector('[data-testid="stApp"]');
                if (!appNode) return;
                var color = window.getComputedStyle(appNode).backgroundColor;
                var match = color.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
                if (match) {
                    var r = parseInt(match[1]), g = parseInt(match[2]), b = parseInt(match[3]);
                    var brightness = (r * 299 + g * 587 + b * 114) / 1000;
                    if (brightness < 128) {
                        parentDoc.documentElement.setAttribute('data-theme', 'dark');
                    } else {
                        parentDoc.documentElement.setAttribute('data-theme', 'light');
                    }
                }
            }
            updateTheme();
            var observer = new MutationObserver(updateTheme);
            observer.observe(window.parent.document.body, { attributes: true, childList: true, subtree: true });
        </script>
        """,
        height=0,
        width=0,
    )


def ensure_data_dir() -> None:
    Path(DATA_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(TARGET_DATA_PATH).parent.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def fetch_audit_log(conn: sqlite3.Connection, limit: int = 250) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            f"SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT {limit}", conn
        )
    except Exception:
        return pd.DataFrame()


def fetch_run_log(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            "SELECT * FROM run_log ORDER BY timestamp DESC LIMIT 200", conn
        )
    except Exception:
        return pd.DataFrame()


def run_script(args: list[str]) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            [sys.executable, *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        return proc.returncode == 0, output.strip()
    except Exception as exc:
        return False, str(exc)


def parse_uploaded_to_df(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".json"):
        payload = json.load(uploaded_file)
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        if isinstance(payload, dict):
            if "records" in payload and isinstance(payload["records"], list):
                return pd.DataFrame(payload["records"])
            return pd.json_normalize(payload)
    raise ValueError("Unsupported format. Upload CSV or JSON.")


def deploy_uploaded_data(uploaded_file, role: str = "source") -> tuple[bool, str]:
    try:
        ensure_data_dir()
        df = parse_uploaded_to_df(uploaded_file)
        if df.empty:
            return False, "Uploaded file is empty."
        if role == "target":
            output_path = TARGET_DATA_PATH
            prefix = "target"
        else:
            output_path = DATA_PATH
            prefix = "source"
        df.to_csv(output_path, index=False)

        st.session_state[f"{prefix}_name"] = uploaded_file.name
        st.session_state[f"{prefix}_rows"] = int(len(df))
        st.session_state[f"{prefix}_cols"] = int(len(df.columns))
        st.session_state[f"{prefix}_deployed_at"] = datetime.utcnow().isoformat()
        return True, f"Deployed {uploaded_file.name} to {output_path} ({len(df)} rows)."
    except Exception as exc:
        return False, str(exc)


def deploy_source_target_pair(source_file, target_file) -> tuple[bool, str]:
    try:
        ensure_data_dir()
        source_df = parse_uploaded_to_df(source_file)
        target_df = parse_uploaded_to_df(target_file)
        if source_df.empty:
            return False, "Source file is empty."
        if target_df.empty:
            return False, "Target file is empty."

        source_df.to_csv(DATA_PATH, index=False)
        target_df.to_csv(TARGET_DATA_PATH, index=False)

        now = datetime.utcnow().isoformat()
        st.session_state["source_name"] = source_file.name
        st.session_state["source_rows"] = int(len(source_df))
        st.session_state["source_cols"] = int(len(source_df.columns))
        st.session_state["source_deployed_at"] = now

        st.session_state["target_name"] = target_file.name
        st.session_state["target_rows"] = int(len(target_df))
        st.session_state["target_cols"] = int(len(target_df.columns))
        st.session_state["target_deployed_at"] = now
        st.session_state["sync_page"] = 1

        return True, (
            f"Checked and deployed source ({source_file.name}) + "
            f"target ({target_file.name})."
        )
    except Exception as exc:
        return False, str(exc)


def load_current_data() -> pd.DataFrame:
    try:
        return pd.read_csv(DATA_PATH)
    except Exception:
        return pd.DataFrame()


def load_target_data() -> pd.DataFrame:
    try:
        return pd.read_csv(TARGET_DATA_PATH)
    except Exception:
        return pd.DataFrame()


def summarize_alignment(source_df: pd.DataFrame, target_df: pd.DataFrame) -> dict:
    if source_df.empty or target_df.empty:
        return {
            "shared_columns": 0,
            "source_only_columns": 0,
            "target_only_columns": 0,
            "row_delta": "N/A",
        }
    source_cols = set(source_df.columns)
    target_cols = set(target_df.columns)
    return {
        "shared_columns": len(source_cols & target_cols),
        "source_only_columns": len(source_cols - target_cols),
        "target_only_columns": len(target_cols - source_cols),
        "row_delta": int(len(source_df) - len(target_df)),
    }


def render_health_plot(run_df: pd.DataFrame) -> None:
    if run_df.empty or "timestamp" not in run_df.columns:
        st.info("No run log data yet.")
        return
    required = {"anomaly_count", "healed_count"}
    if not required.issubset(set(run_df.columns)):
        st.info("Run log does not include enough metrics for chart plotting yet.")
        return

    df = run_df[["timestamp", "anomaly_count", "healed_count"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").tail(60)
    if df.empty:
        st.info("No valid timestamped runs available.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["anomaly_count"],
            mode="lines",
            name="Anomalies",
            line={"shape": "spline", "smoothing": 1.2, "color": "#EF4444", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(239, 68, 68, 0.15)",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["healed_count"],
            mode="lines",
            name="Healed",
            line={"shape": "spline", "smoothing": 1.1, "color": "#38BDF8", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(56, 189, 248, 0.15)",
        )
    )
    fig.update_layout(
        height=320,
        margin={"l": 16, "r": 16, "t": 14, "b": 12},
        showlegend=True,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis={
            "showgrid": False,
            "title": "",
            "tickfont": {"color": "rgba(148, 163, 184, 0.85)", "size": 11},
        },
        yaxis={
            "showgrid": True,
            "gridcolor": "rgba(148, 163, 184, 0.16)",
            "zeroline": False,
            "title": "",
            "tickfont": {"color": "rgba(203, 213, 225, 0.85)", "size": 11},
        },
        hovermode="x unified",
        transition={"duration": 450, "easing": "cubic-in-out"},
        font={"color": "rgba(241, 245, 249, 0.95)"},
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _same_value(left, right) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    return left == right


def build_synced_pages(
    source_df: pd.DataFrame, target_df: pd.DataFrame, page: int, page_size: int
) -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    start = (page - 1) * page_size
    end = start + page_size

    source_slice = source_df.iloc[start:end].reset_index(drop=True)
    target_slice = target_df.iloc[start:end].reset_index(drop=True)
    page_len = max(len(source_slice), len(target_slice), 1)

    source_view = source_slice.reindex(range(page_len))
    target_view = target_slice.reindex(range(page_len))

    row_numbers = [start + i + 1 for i in range(page_len)]
    source_view.insert(0, "_row", row_numbers)
    target_view.insert(0, "_row", row_numbers)

    shared_cols = set(source_df.columns).intersection(set(target_df.columns))
    return source_view, target_view, shared_cols


def style_comparison(
    view_df: pd.DataFrame, other_df: pd.DataFrame, shared_cols: set[str]
) -> pd.io.formats.style.Styler:
    styles = pd.DataFrame("", index=view_df.index, columns=view_df.columns)
    for ridx in view_df.index:
        for col in view_df.columns:
            left = view_df.at[ridx, col]
            right = other_df.at[ridx, col] if col in other_df.columns else pd.NA
            is_green = col == "_row" or (col in shared_cols and _same_value(left, right))
            styles.at[ridx, col] = (
                "background-color: rgba(34,197,94,0.22); color: #0f172a;"
                if is_green
                else "background-color: rgba(239,68,68,0.22); color: #7f1d1d;"
            )
    return view_df.style.apply(lambda _: styles, axis=None)


def _display_cell(value) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def _comparison_rows_html(
    view_df: pd.DataFrame, other_df: pd.DataFrame, shared_cols: set[str]
) -> tuple[str, str]:
    headers = "".join(f"<th>{escape(str(col))}</th>" for col in view_df.columns)
    body_rows: list[str] = []
    for ridx in view_df.index:
        cells: list[str] = []
        for col in view_df.columns:
            left = view_df.at[ridx, col]
            right = other_df.at[ridx, col] if col in other_df.columns else pd.NA
            is_match = col == "_row" or (col in shared_cols and _same_value(left, right))
            klass = "match" if is_match else "mismatch"
            cells.append(f"<td class='{klass}'>{escape(_display_cell(left))}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return headers, "".join(body_rows)


def render_synced_tables(
    source_view: pd.DataFrame, target_view: pd.DataFrame, shared_cols: set[str], height: int = 460
) -> None:
    src_headers, src_rows = _comparison_rows_html(source_view, target_view, shared_cols)
    tgt_headers, tgt_rows = _comparison_rows_html(target_view, source_view, shared_cols)
    html = f"""
    <style>
      .sync-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      }}
      .sync-grid > div {{
        min-width: 0;
      }}
      .sync-title {{
        font-weight: 700;
        font-size: 18px;
        margin: 2px 0 8px 0;
        color: #1f2937;
      }}
      .sync-wrap {{
        max-height: {height}px;
        overflow: auto;
        overscroll-behavior: contain;
        border: 1px solid #bcbaaf;
        border-radius: 12px;
        background: #efeddd;
        width: 100%;
      }}
      .sync-table {{
        width: max-content;
        min-width: 100%;
        border-collapse: collapse;
        font-size: 13px;
      }}
      .sync-table thead th {{
        position: sticky;
        top: 0;
        z-index: 2;
        padding: 8px 10px;
        border-bottom: 1px solid #bcbaaf;
        background: #d8d6c9;
        text-align: left;
        white-space: nowrap;
        color: #111827;
        min-width: 120px;
      }}
      .sync-table td {{
        padding: 6px 10px;
        border-bottom: 1px solid #c9c7bb;
        white-space: nowrap;
        color: #111827;
        min-width: 120px;
      }}
      .sync-table td.match {{
        background: rgba(34, 197, 94, 0.20);
        color: #14532d;
      }}
      .sync-table td.mismatch {{
        background: rgba(239, 68, 68, 0.20);
        color: #7f1d1d;
      }}
    </style>
    <div class="sync-grid">
      <div>
        <div class="sync-title">Source (Synced)</div>
        <div class="sync-wrap" id="source-wrap">
          <table class="sync-table">
            <thead><tr>{src_headers}</tr></thead>
            <tbody>{src_rows}</tbody>
          </table>
        </div>
      </div>
      <div>
        <div class="sync-title">Target (Synced)</div>
        <div class="sync-wrap" id="target-wrap">
          <table class="sync-table">
            <thead><tr>{tgt_headers}</tr></thead>
            <tbody>{tgt_rows}</tbody>
          </table>
        </div>
      </div>
    </div>
    <script>
      const source = document.getElementById("source-wrap");
      const target = document.getElementById("target-wrap");
      let syncing = false;
      function mirror(primary, secondary) {{
        if (syncing) return;
        syncing = true;
        secondary.scrollLeft = primary.scrollLeft;
        secondary.scrollTop = primary.scrollTop;
        requestAnimationFrame(() => {{
          syncing = false;
        }});
      }}
      if (source && target) {{
        source.addEventListener("scroll", () => mirror(source, target), {{ passive: true }});
        target.addEventListener("scroll", () => mirror(target, source), {{ passive: true }});
      }}
    </script>
    """
    components.html(html, height=height + 70, scrolling=False)


def render_plain_table(df: pd.DataFrame, max_rows: int = 200) -> None:
    if df.empty:
        st.markdown(
            "<div class='panel' style='min-height:auto;'><div class='meta'>No rows available.</div></div>",
            unsafe_allow_html=True,
        )
        return
    view = df.head(max_rows).copy()
    headers = "".join(f"<th>{escape(str(col))}</th>" for col in view.columns)
    body_rows: list[str] = []
    for _, row in view.iterrows():
        cells = "".join(f"<td>{escape(_display_cell(v))}</td>" for v in row.tolist())
        body_rows.append(f"<tr>{cells}</tr>")
    rows_html = "".join(body_rows)
    st.markdown(
        (
            "<div class='plain-table-wrap'>"
            "<table class='plain-table'>"
            f"<thead><tr>{headers}</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def compute_kpis(audit_df: pd.DataFrame, run_df: pd.DataFrame, data_df: pd.DataFrame) -> dict:
    total_checks = len(audit_df) if not audit_df.empty else 0
    healed_count = int((audit_df["outcome"] == "HEALED").sum()) if not audit_df.empty else 0
    unresolvable_count = int((audit_df["outcome"] == "UNRESOLVABLE").sum()) if not audit_df.empty else 0
    heal_rate = round((healed_count / total_checks) * 100, 1) if total_checks else 0.0

    row_count = int(len(data_df)) if not data_df.empty else 0
    col_count = int(len(data_df.columns)) if not data_df.empty else 0
    null_cells = int(data_df.isna().sum().sum()) if not data_df.empty else 0
    total_cells = int(data_df.shape[0] * data_df.shape[1]) if not data_df.empty else 0
    null_rate = round((null_cells / total_cells) * 100, 2) if total_cells else 0.0

    codex_calls = int(run_df["codex_calls"].sum()) if not run_df.empty else 0
    anomaly_events = int(run_df["anomaly_count"].sum()) if (not run_df.empty and "anomaly_count" in run_df.columns) else 0

    last_run = "N/A"
    if not run_df.empty and "timestamp" in run_df.columns:
        try:
            ts = str(run_df.iloc[0]["timestamp"])
            last_run = ts[:19].replace("T", " ")
        except Exception:
            pass

    uptime = "0m"
    if not run_df.empty and "timestamp" in run_df.columns:
        try:
            first_run = datetime.fromisoformat(str(run_df["timestamp"].min()))
            delta = datetime.utcnow() - first_run
            hours = int(delta.total_seconds() // 3600)
            mins = int((delta.total_seconds() % 3600) // 60)
            uptime = f"{hours}h {mins}m"
        except Exception:
            uptime = "N/A"

    return {
        "rows": row_count,
        "cols": col_count,
        "null_rate": null_rate,
        "total_checks": total_checks,
        "healed": healed_count,
        "unresolvable": unresolvable_count,
        "heal_rate": heal_rate,
        "codex_calls": codex_calls,
        "anomaly_events": anomaly_events,
        "last_run": last_run,
        "uptime": uptime,
    }


def compute_trust_score(col_name: str, audit_df: pd.DataFrame) -> int:
    if audit_df.empty:
        return 100
    col_events = audit_df[audit_df["column_name"] == col_name].head(10)
    if col_events.empty:
        return 100
    healed = (col_events["outcome"] == "HEALED").sum()
    unresolvable = (col_events["outcome"] == "UNRESOLVABLE").sum()
    total = len(col_events)
    score = int(100 - (unresolvable / total * 60) - (healed / total * 10))
    return max(0, min(100, score))


def trust_color(score: int) -> str:
    if score >= 80:
        return "#22c55e"
    if score >= 50:
        return "#f59e0b"
    return "#ef4444"


def render_sidebar_nav() -> str:
    st.sidebar.markdown("<h2 style='color: var(--cyan);'>🦝 RacoonDQ</h2>", unsafe_allow_html=True)
    st.sidebar.title("Navigation")
    nav_mode = st.sidebar.radio("Pages", ["Home", "Test"], index=0)
    return nav_mode

def render_automatic_testing() -> None:
    st.markdown("<div class='section-title'>Automatic Test Pipeline</div>", unsafe_allow_html=True)
    st.caption("Deploy first, then run the test pipeline to safely generate and inject anomalous scenarios.")

    if "test_deployed" not in st.session_state:
        st.session_state["test_deployed"] = False

    if st.button("Deploy Testing Framework", use_container_width=True):
        st.session_state["test_deployed"] = True
        st.success("Testing deployed. You can now configure scenarios and run the pipeline.")
        st.rerun()

    c1, c2 = st.columns(2)
    with c1:
        scenario = st.selectbox(
            "Pipeline Scenario",
            ["random", "null", "type", "dupe", "range"],
            index=0,
            disabled=not st.session_state["test_deployed"],
        )
    with c2:
        run_check_after = st.checkbox(
            "Run Overall Check After Pipeline",
            value=True,
            disabled=not st.session_state["test_deployed"],
        )

    if st.button(
        "Run Test Pipeline (Generate + Inject)",
        use_container_width=True,
        type="primary",
        disabled=not st.session_state["test_deployed"],
    ):
        with st.status("Initializing Automatic Test Pipeline...", expanded=True) as status:
            logs: list[str] = []

            # Step 1: Generate Data
            status.update(label="Step 1: Generating standard mock data...", state="running")
            ok_seed, out_seed = run_script(["seed_data.py"])
            if not ok_seed:
                status.update(label="Pipeline failed at generate step.", state="error")
                st.error("Error generating dummy data.")
                st.code(out_seed)
                return
            st.write("✅ Data generated successfully.")
            logs.append("== Generate Dummy Data ==\n" + (out_seed or "(no output)"))

            # Step 2: Inject Anomalies
            status.update(label="Step 2: Injecting realistic bad data anomalies...", state="running")
            inject_args = ["inject_bad.py"]
            if scenario != "random":
                inject_args.extend(["--scenario", scenario])
            ok_inject, out_inject = run_script(inject_args)
            if not ok_inject:
                status.update(label="Pipeline failed at inject step.", state="error")
                st.error("Error injecting bad data.")
                st.code(out_inject)
                return
            st.write(f"✅ Bad data injected successfully (Scenario: {scenario}).")
            logs.append("== Inject Bad Data ==\n" + (out_inject or "(no output)"))

            # Step 3: Deploy to Engine
            status.update(label="Step 3: Providing Active Source to Engine...", state="running")
            try:
                test_df = pd.read_csv(DATA_PATH)
                conn = db.get_connection(DB_PATH)
                test_df.to_sql("table_data", conn, if_exists="replace", index=False)
                st.session_state["source_name"] = "Automatic Test Payload"
                st.session_state["source_rows"] = int(len(test_df))
                st.session_state["source_cols"] = int(len(test_df.columns))
                st.session_state["source_deployed_at"] = pd.Timestamp.now().isoformat()
                import time
                time.sleep(0.5)
                st.write("✅ Data payload provided and deployed to active engine.")
            except Exception as e:
                status.update(label="Pipeline failed during deployment.", state="error")
                st.error(f"Error deploying test payload: {str(e)}")
                return

            # Step 4: Codex Self Healing
            if run_check_after:
                status.update(label="Step 4: Running deep LLM self-healing via Codex...", state="running")
                try:
                    summary = run_once(conn)
                    logs.append(f"== Overall Check Summary ==\n{summary}")
                    st.write("✅ Self healing with help of Codex completed successfully. Agents are working.")
                except Exception as exc:
                    status.update(label="Pipeline failed during Codex LLM healing.", state="error")
                    st.error(f"Agent Execution Error: {exc}")
                    logs.append(f"== Overall Check Error ==\n{exc}")
                    return
            
            status.update(label="Pipeline working successfully!", state="complete")
        
        st.success("Test Pipeline completely finished. Navigate to 'Home' to view Auto-Healed data matrix.")
        
    st.markdown("---")
    c3, c4 = st.columns(2)
    with c3:
        st.session_state["show_listing"] = st.checkbox(
            "Show Data Listing Preview",
            value=st.session_state.get("show_listing", False),
        )
    with c4:
        if st.button("Clear Audit History", use_container_width=True):
            try:
                conn = db.get_connection(DB_PATH)
                conn.execute("DELETE FROM audit_log")
                conn.execute("DELETE FROM run_log")
                conn.commit()
                st.success("Audit history cleared.")
            except Exception as exc:
                st.error(str(exc))


def render_home() -> None:
    conn = get_conn()
    audit_df = fetch_audit_log(conn)
    run_df = fetch_run_log(conn)
    data_df = load_current_data()
    target_df = load_target_data()
    kpi = compute_kpis(audit_df, run_df, data_df)
    alignment = summarize_alignment(data_df, target_df)

    try:
        with open(r"C:\Users\AkashGhosh\Downloads\files (3)\racoon-removebg-preview.png", "rb") as img_f:
            b64_logo = base64.b64encode(img_f.read()).decode("utf-8")
            logo_src = f"data:image/png;base64,{b64_logo}"
    except Exception:
        logo_src = "https://images.unsplash.com/photo-1549471013-3364d7220b75?auto=format&fit=crop&w=150&q=80"

    st.markdown(
        f"""
        <div style='display: flex; justify-content: space-between; align-items: center; padding: 16px 24px; background: var(--surface); border-radius: 16px; border: 1px solid var(--border); box-shadow: var(--shadow); margin-bottom: 24px; backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);'>
            <div style='font-weight: 800; font-size: 20px; color: var(--text); display: flex; align-items: center; gap: 12px;'>
                <img src="{logo_src}" style="width: 32px; height: 32px; border-radius: 8px; box-shadow: var(--glow); object-fit: cover; background: var(--surface);" alt="Racoon Logo" />
                RACOONDQ <span style='font-weight: 400; color: var(--muted); padding-left: 8px; border-left: 1px solid var(--border); margin-left: 8px;'>Analytics Platform</span>
            </div>
            <div style='display: flex; gap: 24px; font-size: 14px; font-weight: 600; color: var(--text-soft);'>
                <span style='cursor: pointer; color: var(--cyan); text-shadow: var(--glow);'>Overview</span>
                <span style='cursor: pointer; hover: color: var(--cyan);'>Integrations</span>
                <span style='cursor: pointer; hover: color: var(--cyan);'>Settings</span>
            </div>
        </div>
        
        <div class='hero'>
            <div style='font-size: 14px; text-transform: uppercase; letter-spacing: 2px; color: var(--cyan); margin-bottom: 8px; font-weight: 700;'>v2.0 Self-Healing Architecture</div>
            <h1>RacoonDQ Autonomous Agent</h1>
            <p>Real-time anomaly detection, zero-latency synchronization, and AI-driven predictive healing.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _render_manual_testing_ui(conn, data_df, target_df, kpi, alignment, run_df, audit_df)

def _render_manual_testing_ui(conn, data_df, target_df, kpi, alignment, run_df, audit_df) -> None:
    st.markdown("<div class='section-title'>Audit DQ</div>", unsafe_allow_html=True)
    st.markdown("<div class='meta' style='margin-bottom: 20px;'>Deploy a single table dataset for deep analysis and LLM Auto-Healing.</div>", unsafe_allow_html=True)
    c1, c2 = st.columns([1.2, 1])
    
    with c1:
        active_upload = st.file_uploader(
            "Upload Source Data (CSV/JSON)",
            type=["csv", "json"],
            key="active_upload",
        )
        if st.button("Deploy to Engine (Source)", use_container_width=True, disabled=(active_upload is None)):
            with st.spinner("Deploying data..."):
                ok, msg = deploy_uploaded_data(active_upload, "source")
            if ok:
                st.session_state["analysis_complete"] = False
                st.session_state["analysis_reason"] = ""
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
                
    with c2:
        source_name = st.session_state.get("source_name", "No source deployed")
        source_rows = st.session_state.get("source_rows", kpi["rows"])
        source_cols = st.session_state.get("source_cols", kpi["cols"])
        deployed_at = st.session_state.get("source_deployed_at", "N/A")
        st.markdown(
            f"""
            <div class='panel' style='background: rgba(56, 189, 248, 0.05); border-color: rgba(56, 189, 248, 0.15); margin-bottom: 12px;'>
                <h4 style="color: var(--cyan); margin-bottom: 8px;">🔥 Active Source Dataset</h4>
                <div class='value'>{source_name}</div>
                <div class='meta'>Rows: {source_rows} | Columns: {source_cols} | Deployed at: {str(deployed_at)[:19].replace('T', ' ')}</div>
            </div>
            """, unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Reconciliation Data Loader</div>", unsafe_allow_html=True)
    st.markdown("<div class='meta' style='margin-bottom: 20px;'>Compare an Active Source against a Target payload. <strong>Both must be deployed for synchronization.</strong></div>", unsafe_allow_html=True)
    c_recon1, c_recon2 = st.columns([1.2, 1])

    with c_recon1:
        target_upload = st.file_uploader(
            "Upload Target Data (CSV/JSON)",
            type=["csv", "json"],
            key="target_upload",
        )
        if st.button("Deploy Target Payload", use_container_width=True, disabled=(target_upload is None)):
            with st.spinner("Deploying target dataset..."):
                ok, msg = deploy_uploaded_data(target_upload, "target")
            if ok:
                st.session_state["analysis_complete"] = False
                st.session_state["analysis_reason"] = ""
                st.success("Target deployed successfully.")
                st.rerun()
            else:
                st.error(msg)
                
    with c_recon2:
        target_name = st.session_state.get("target_name", "No target deployed")
        target_rows = st.session_state.get("target_rows", int(len(target_df)) if not target_df.empty else 0)
        target_cols = st.session_state.get("target_cols", int(len(target_df.columns)) if not target_df.empty else 0)
        target_deployed_at = st.session_state.get("target_deployed_at", "N/A")
        st.markdown(
            f"""
            <div class='panel' style='background: rgba(168, 85, 247, 0.05); border-color: rgba(168, 85, 247, 0.15); margin-bottom: 12px;'>
                <h4 style="color: var(--purple); margin-bottom: 8px;">⚖️ Target Compare Dataset</h4>
                <div class='value'>{target_name}</div>
                <div class='meta'>Rows: {target_rows} | Columns: {target_cols} | Deployed at: {str(target_deployed_at)[:19].replace('T', ' ')}</div>
            </div>
            """, unsafe_allow_html=True
        )

    st.markdown(
        f"""
        <div class='panel' style='margin-top: 12px;'>
            <h4>Source vs Target Alignment</h4>
            <div class='meta'>Shared columns: {alignment['shared_columns']}</div>
            <div class='meta'>Source-only columns: {alignment['source_only_columns']}</div>
            <div class='meta'>Target-only columns: {alignment['target_only_columns']}</div>
            <div class='meta'>Row delta (source-target): {alignment['row_delta']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("---")
    col_synced1, col_synced2 = st.columns([8, 2])
    with col_synced1:
        st.markdown("<div class='section-title'>Synced View</div>", unsafe_allow_html=True)
    with col_synced2:
        if "analysis_complete" not in st.session_state:
            st.session_state["analysis_complete"] = False
            st.session_state["analysis_reason"] = ""

        if not st.session_state.get("analysis_complete", False):
            if st.button("🔍 Analyse Dataset", type="primary", use_container_width=True, disabled=data_df.empty):
                with st.spinner("Analyzing dataset anomalies..."):
                    import time
                    time.sleep(0.6)  # Give visual feedback
                    null_cols = data_df.columns[data_df.isnull().any()].tolist()
                    if null_cols:
                        reason = f"Detected missing values in: {', '.join(null_cols)}. Agent will perform deterministic LLM imputation."
                    else:
                        reason = "Format checks passed. Agent will run deep LLM validation to strictly verify constraints."
                    st.session_state["analysis_complete"] = True
                    st.session_state["analysis_reason"] = reason
                    st.rerun()
        else:
            if st.button("🪄 Run Self-Healing Agent", type="primary", use_container_width=True, help=st.session_state.get("analysis_reason", "Ready")):
                with st.spinner("RacoonDQ LLM Engine running validation & healing..."):
                    import time
                    time.sleep(0.4)
                    try:
                        summary = run_once(conn)
                        st.session_state["analysis_complete"] = False
                    except Exception as e:
                        st.error(f"Execution failed: {e}")
                with st.spinner("Reloading dashboard..."):
                    time.sleep(0.5)
                    st.success(f"Agent cycle complete!")
                    st.rerun()

    if not data_df.empty and not target_df.empty:
        if "sync_page" not in st.session_state:
            st.session_state["sync_page"] = 1
        if "sync_page_size" not in st.session_state:
            st.session_state["sync_page_size"] = 10

        total_rows = max(len(data_df), len(target_df))
        page_size = st.selectbox(
            "Rows per page",
            options=[10, 15],
            index=0 if st.session_state.get("sync_page_size", 10) == 10 else 1,
        )
        st.session_state["sync_page_size"] = page_size

        total_pages = max(1, math.ceil(total_rows / page_size))
        current_page = min(int(st.session_state.get("sync_page", 1)), total_pages)
        st.session_state["sync_page"] = current_page

        pager_cols = st.columns([1, 1, 1, 1, 2])
        for p in range(1, min(3, total_pages) + 1):
            if pager_cols[p - 1].button(str(p), key=f"sync_page_{p}", use_container_width=True):
                st.session_state["sync_page"] = p
                st.rerun()
        if pager_cols[3].button(
            "Next",
            key="sync_page_next",
            disabled=current_page >= total_pages,
            use_container_width=True,
        ):
            st.session_state["sync_page"] = current_page + 1
            st.rerun()
        pager_cols[4].markdown(
            f"<div style='padding-top:8px; color: var(--muted);'>Page {current_page} / {total_pages}</div>",
            unsafe_allow_html=True,
        )

        source_view, target_view, shared_cols = build_synced_pages(
            data_df, target_df, current_page, page_size
        )
        render_synced_tables(source_view, target_view, shared_cols, height=460)
        st.caption("Green = matching values. Red = mismatch or missing counterpart.")
    elif not data_df.empty:
        st.caption("Active DQ Dataset (Target not uploaded)")
        st.dataframe(data_df, use_container_width=True, height=460)
    elif not target_df.empty:
        st.caption("Target Compare Dataset (Source not uploaded)")
        st.dataframe(target_df, use_container_width=True, height=460)
    else:
        st.info("Upload source or target data to view dataset previews here.")


    # SectionCards (Hackathon style mapping)
    st.markdown("<div class='section-title' style='margin-bottom: 20px;'><span style='color: var(--cyan);'>//</span> Main Telemetry</div>", unsafe_allow_html=True)
    st.markdown(
        f"""
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px;">
            <div class="kpi-card"><div class="kpi-value">{kpi['rows']:,}</div><div class="kpi-title">Total Rows</div></div>
            <div class="kpi-card"><div class="kpi-value">{kpi['cols']:,}</div><div class="kpi-title">Data Dimensions</div></div>
            <div class="kpi-card"><div class="kpi-value" style="background: linear-gradient(135deg, var(--ok) 0%, var(--cyan) 100%); -webkit-background-clip: text;">{kpi['heal_rate']}%</div><div class="kpi-title">Auto-Heal Rate</div></div>
            <div class="kpi-card"><div class="kpi-value" style="background: linear-gradient(135deg, var(--bad) 0%, var(--warn) 100%); -webkit-background-clip: text;">{kpi['unresolvable']:,}</div><div class="kpi-title">Critical Anomalies</div></div>
            <div class="kpi-card"><div class="kpi-value">{kpi['null_rate']}%</div><div class="kpi-title">Missing Data Rate</div></div>
            <div class="kpi-card"><div class="kpi-value">{kpi['total_checks']:,}</div><div class="kpi-title">Integrity Checks</div></div>
            <div class="kpi-card"><div class="kpi-value">{kpi['anomaly_events']:,}</div><div class="kpi-title">Detected Fluctuations</div></div>
            <div class="kpi-card"><div class="kpi-value" style="background: linear-gradient(135deg, var(--purple) 0%, var(--cyan) 100%); -webkit-background-clip: text;">{kpi['codex_calls']:,}</div><div class="kpi-title">LLM Inference Calls</div></div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("---")

    # ChartAreaInteractive (shadcn style mapping)
    st.markdown("<div class='section-title'>Chart Area Interactive</div>", unsafe_allow_html=True)
    chart_wrap = st.container(border=True)
    with chart_wrap:
        chart_mode = st.selectbox(
            "Metric",
            ["Run Health", "Outcome Count", "Column Nulls"],
            index=0,
            help="Switch chart context like an interactive chart area.",
        )

        if chart_mode == "Run Health":
            render_health_plot(run_df)
        elif chart_mode == "Outcome Count":
            if not audit_df.empty:
                counts = audit_df["outcome"].value_counts()
                fig = go.Figure(
                    data=[
                        go.Bar(
                            x=counts.index.tolist(),
                            y=counts.values.tolist(),
                            marker={
                                "color": ["#38BDF8", "#10B981", "#EF4444"]
                            },
                            hovertemplate="%{x}: %{y}<extra></extra>",
                        )
                    ]
                )
                fig.update_layout(
                    height=320,
                    margin={"l": 16, "r": 16, "t": 14, "b": 12},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis={"showgrid": False, "title": ""},
                    yaxis={"showgrid": True, "gridcolor": "rgba(148, 163, 184, 0.16)", "title": ""},
                    font={"color": "rgba(148, 163, 184, 0.95)"},
                    transition={"duration": 350, "easing": "cubic-in-out"},
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.info("No audit outcomes yet.")
        else:
            if not data_df.empty:
                nulls = data_df.isna().sum().sort_values(ascending=False).head(12)
                fig = go.Figure(
                    data=[
                        go.Bar(
                            x=nulls.index.tolist(),
                            y=nulls.values.tolist(),
                            marker={"color": "#6D28D9"},
                            hovertemplate="%{x}: %{y}<extra></extra>",
                        )
                    ]
                )
                fig.update_layout(
                    height=320,
                    margin={"l": 16, "r": 16, "t": 14, "b": 12},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis={"showgrid": False, "title": ""},
                    yaxis={"showgrid": True, "gridcolor": "rgba(148, 163, 184, 0.16)", "title": ""},
                    font={"color": "rgba(148, 163, 184, 0.95)"},
                    transition={"duration": 350, "easing": "cubic-in-out"},
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.info("No source data loaded.")

    st.markdown("---")

    # DataTable (shadcn style mapping)
    st.markdown("<div class='section-title'>Data Table</div>", unsafe_allow_html=True)
    tab1, tab2, tab3, tab4 = st.tabs(["Audit Events", "Current Dataset", "Target Dataset", "Column Trust"])
    with tab1:
        if not audit_df.empty:
            cols = [
                "timestamp",
                "file_name",
                "column_name",
                "check_type",
                "anomaly_detail",
                "affected_rows",
                "outcome",
                "diagnosis",
                "fix_code"
            ]
            cols = [c for c in cols if c in audit_df.columns]
            render_plain_table(audit_df[cols], max_rows=200)
            st.caption("Showing up to 200 latest audit rows.")
        else:
            st.markdown(
                "<div class='panel' style='min-height:auto;'><div class='meta'>No audit events available.</div></div>",
                unsafe_allow_html=True,
            )
    with tab2:
        if not data_df.empty:
            render_plain_table(data_df, max_rows=200)
            st.caption("Showing up to 200 rows from current source dataset.")
        else:
            st.markdown(
                "<div class='panel' style='min-height:auto;'><div class='meta'>No source dataset found. Upload and check source + target files.</div></div>",
                unsafe_allow_html=True,
            )
    with tab3:
        if not target_df.empty:
            render_plain_table(target_df, max_rows=200)
            st.caption("Showing up to 200 rows from target dataset.")
        else:
            st.markdown(
                "<div class='panel' style='min-height:auto;'><div class='meta'>No target dataset found. Upload and check source + target files.</div></div>",
                unsafe_allow_html=True,
            )
    with tab4:
        if not data_df.empty:
            trust_rows = []
            for col_name in data_df.columns:
                trust_rows.append(
                    {
                        "column": col_name,
                        "trust_score": compute_trust_score(col_name, audit_df),
                        "null_rate": round(float(data_df[col_name].isna().mean()) * 100, 2),
                        "dtype": str(data_df[col_name].dtype),
                    }
                )
            trust_df = pd.DataFrame(trust_rows).sort_values("trust_score", ascending=False)
            render_plain_table(trust_df, max_rows=200)
        else:
            st.markdown(
                "<div class='panel' style='min-height:auto;'><div class='meta'>No columns to score yet.</div></div>",
                unsafe_allow_html=True,
            )

    st.markdown(
        f"<p style='color:#64748b; font-size:11px; text-align:right;'>"
        f"Home auto-refresh: every {REFRESH_INTERVAL}s"
        f"</p>",
        unsafe_allow_html=True,
    )


def render_test_page() -> None:
    st.title("Testing Panel")
    st.markdown("Use the sidebar Test controls to generate/inject scenarios safely.")
    if st.session_state.get("show_listing", False):
        st.subheader("Current Data Listing (Top 25 Rows)")
        try:
            listing_df = pd.read_csv(DATA_PATH).head(25)
            st.dataframe(listing_df, use_container_width=True, height=320)
        except Exception as exc:
            st.error(f"Could not load current data: {exc}")
    else:
        st.info("Enable `Show Data Listing` in the sidebar to preview data.")


@st.fragment(run_every=f"{REFRESH_INTERVAL}s")
def render_home_live() -> None:
    render_home()

inject_styles()
mode = render_sidebar_nav()
if mode == "Home":
    render_home_live()
elif mode == "Test":
    render_automatic_testing()
    render_test_page()
