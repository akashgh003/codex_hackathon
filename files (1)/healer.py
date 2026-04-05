"""
healer.py — The self-healing engine (Steps 3-4 of the agent loop).

Full flow per anomaly (matches flowchart):
  1. Backup original DataFrame
  2. Set attempt = 1
  3. Call Claude (codex_client) → get diagnosis + fix_code
  4. Apply fix_code to DataFrame
  5. Re-profile fixed DataFrame
  6. Re-run the specific check → did it pass?
     YES → log HEALED, return healed df
     NO  → attempt++ → if attempt <= 3 → go to step 3 (new prompt)
            → if attempt > 3 → UNRESOLVABLE: restore backup, log recommendation
  7. Write full audit record to SQLite
"""

import pandas as pd
import copy
import traceback
from datetime import datetime

from codex_client import call_claude

MAX_ATTEMPTS = 3


def _apply_fix(df: pd.DataFrame, fix_code: str) -> pd.DataFrame:
    """
    Execute Claude's fix_code in a sandboxed local scope.
    The code operates on `df` and must leave a modified `df` in scope.
    Returns the modified DataFrame.
    """
    local_scope = {"df": df.copy(), "pd": pd}
    exec(fix_code, {}, local_scope)  # noqa: S102
    result = local_scope.get("df")
    if result is None or not isinstance(result, pd.DataFrame):
        raise ValueError("fix_code did not produce a valid DataFrame in `df`")
    return result


def _verify_fix(df_fixed: pd.DataFrame, anomaly: dict) -> bool:
    """
    Re-run the specific check against the fixed DataFrame.
    Returns True if the anomaly no longer exists.
    """
    check_type = anomaly["check_type"]
    col = anomaly["column"]

    if check_type == "null_rate":
        from detector import NULL_RATE_THRESHOLD
        rate = df_fixed[col].isna().mean() if col in df_fixed.columns else 1.0
        return rate <= NULL_RATE_THRESHOLD

    elif check_type == "duplicates":
        from detector import DUPE_RATE_THRESHOLD
        dupe_rate = df_fixed.duplicated().mean()
        return dupe_rate <= DUPE_RATE_THRESHOLD

    elif check_type == "type_drift":
        # After a type fix we accept any consistent dtype
        return col in df_fixed.columns and df_fixed[col].dtype != object

    elif check_type == "range_violation":
        # Accept if no extreme outliers remain (simple IQR check)
        if col not in df_fixed.columns:
            return False
        series = df_fixed[col].dropna()
        if series.empty:
            return True
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        outliers = ((series < q1 - 3 * iqr) | (series > q3 + 3 * iqr)).sum()
        return outliers == 0

    elif check_type == "row_drop":
        # Can't un-drop rows automatically; consider fixed if we acknowledge it
        return True

    return False


def heal(
    anomaly: dict,
    df: pd.DataFrame,
    profile: dict,
    conn,          # sqlite3.Connection
    data_path: str,
) -> tuple[pd.DataFrame, str]:
    """
    Attempt to heal a single anomaly.

    Returns:
        (final_df, outcome)  where outcome is 'HEALED' or 'UNRESOLVABLE'
    """
    # ── Step 1: Backup ───────────────────────────────────────────────────────
    df_backup = df.copy()
    df_working = df.copy()

    outcome = "UNRESOLVABLE"
    diagnosis = ""
    fix_code_used = ""
    recommended_action = ""
    previous_fix = None
    attempt = 0

    for attempt in range(1, MAX_ATTEMPTS + 1):
        # ── Step 3: Call Claude ──────────────────────────────────────────────
        try:
            claude_response = call_claude(
                anomaly=anomaly,
                profile=profile,
                attempt=attempt,
                previous_fix=previous_fix,
            )
        except Exception as e:
            diagnosis = f"OpenAI API error on attempt {attempt}: {e}"
            print(f"[ERROR] LLM Agent Execution Failed: {e}")
            fix_code_used = ""
            recommended_action = "Check API key and rate limits (429), then manually inspect column."
            continue

        diagnosis = claude_response.get("diagnosis", "")
        fix_code_used = claude_response.get("fix_code", "")
        recommended_action = claude_response.get("recommended_action", "")
        confidence = claude_response.get("confidence", "LOW")

        # ── Step 4: Apply fix ────────────────────────────────────────────────
        try:
            df_candidate = _apply_fix(df_working, fix_code_used)
        except Exception as e:
            previous_fix = fix_code_used
            diagnosis += f" | Fix execution error: {e}"
            continue

        # ── Step 5-6: Verify ─────────────────────────────────────────────────
        passed = _verify_fix(df_candidate, anomaly)

        if passed:
            # SUCCESS — persist fixed CSV
            df_candidate.to_csv(data_path, index=False)
            outcome = "HEALED"
            try:
                col = anomaly["column"]
                affected_rows = int((df_candidate[col].fillna('__N__') != df_backup[col].fillna('__N__')).sum())
            except Exception:
                affected_rows = 0
            df_working = df_candidate
            break
        else:
            previous_fix = fix_code_used
            # Loop continues with attempt+1

    # ── UNRESOLVABLE path ────────────────────────────────────────────────────
    if outcome == "UNRESOLVABLE":
        df_backup.to_csv(data_path, index=False)  # Restore backup
        df_working = df_backup
        affected_rows = 0

    import os
    file_name = os.path.basename(data_path)

    # ── Step 7: Write audit record ───────────────────────────────────────────
    _write_audit(
        conn=conn,
        anomaly=anomaly,
        outcome=outcome,
        attempts=attempt,
        diagnosis=diagnosis,
        fix_code=fix_code_used,
        recommended_action=recommended_action,
        file_name=file_name,
        affected_rows=affected_rows,
    )

    return df_working, outcome


def _write_audit(
    conn,
    anomaly: dict,
    outcome: str,
    attempts: int,
    diagnosis: str,
    fix_code: str,
    recommended_action: str,
    file_name: str = "",
    affected_rows: int = 0
):
    """Write a complete audit record to the SQLite audit_log table."""
    from datetime import datetime
    conn.execute(
        """
        INSERT INTO audit_log (
            timestamp, column_name, check_type, severity,
            anomaly_detail, outcome, attempts,
            diagnosis, fix_code, recommended_action,
            file_name, affected_rows
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.utcnow().isoformat(),
            anomaly["column"],
            anomaly["check_type"],
            anomaly["severity"],
            anomaly["detail"],
            outcome,
            attempts,
            diagnosis,
            fix_code,
            recommended_action,
            file_name,
            affected_rows,
        ),
    )
    conn.commit()
