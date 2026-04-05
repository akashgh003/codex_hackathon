"""
codex_client.py - The ONLY LLM API call in the system.
Given an anomaly + dataset profile, asks OpenAI to:
  1. Diagnose the root cause
  2. Return an executable Python fix (pandas operations on `df`)

Returns a structured dict:
  {
    "diagnosis": str,
    "fix_code": str,
    "confidence": str,     # HIGH / MEDIUM / LOW
    "recommended_action": str
  }
"""

import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
MAX_TOKENS = 1000
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()


def build_prompt(anomaly: dict, profile: dict, attempt: int = 1, previous_fix: str = None) -> str:
    """
    Construct the prompt sent to the model.
    On retry (attempt > 1), include failed fix context.
    """
    col = anomaly["column"]
    col_stats = profile["columns"].get(col, {})

    prev_context = ""
    if attempt > 1 and previous_fix:
        prev_context = f"""
PREVIOUS FIX ATTEMPT (attempt {attempt - 1}) FAILED:
```python
{previous_fix}
```
Do NOT repeat this approach. Try a fundamentally different strategy.
"""

    prompt = f"""You are a Principal Data Quality Engineer operating inside an automated data pipeline.

You are given a pandas DataFrame `df`. Your task is to detect and fix ALL data quality issues dynamically based on the data itself.

You MUST generate ONLY executable pandas code using `.loc` or vectorized operations.

---
## STRICT RULES
1. DO NOT skip any issue.
2. DO NOT mark anything as unresolvable.
3. DO NOT drop rows unless values are completely irrecoverable across ALL columns.
4. DO NOT modify the DataFrame index.
5. DO NOT hardcode column names or values unless inferred from data.
6. Infer everything from column data types and patterns.
7. Output ONLY pandas code (no explanation).

---
## STEP 1: INFER COLUMN TYPES
For each column:
* Detect if it is:
  * numeric (int/float-like, even if stored as string)
  * datetime (multiple formats possible)
  * categorical (low cardinality text)
  * free text (high cardinality)

---
## STEP 2: HANDLE MISSING VALUES
* Numeric columns → fill with median
* Datetime columns → forward fill, then backward fill if needed
* Categorical columns:
  * If a dominant value exists → fill with mode
  * If another column logically determines it (dependency detected) → infer from that column
* If no inference possible → fill with "UNKNOWN" (string columns)

---
## STEP 3: CLEAN AND CAST NUMERIC DATA
* Remove non-numeric characters (currency symbols, text, spaces)
* Convert to numeric (float)
* Invalid parsing → set as NaN → then apply median imputation

---
## STEP 4: STANDARDIZE DATETIME
* Parse all date formats using pandas
* Convert to datetime64[ns]
* Standardize format to YYYY-MM-DD
* Invalid dates → treat as NaT → fill using forward/backward fill

---
## STEP 5: STANDARDIZE CATEGORICAL VALUES
* Strip leading/trailing spaces
* Normalize casing (convert to uppercase)
* Detect similar values using fuzzy/string similarity and merge them into dominant category
* Remove duplicates caused by typos or spacing issues

---
## STEP 6: OUTLIER DETECTION & HANDLING
For numeric columns:
* Detect outliers using IQR or percentile method
* Values outside reasonable bounds:
  * Option 1: cap to lower/upper bounds
  * Option 2: set to NaN and re-impute with median
* Negative values in logically non-negative columns → convert to absolute or nullify

---
## STEP 7: FINAL TYPE ENFORCEMENT
* Ensure:
  * numeric columns → int/float
  * datetime columns → datetime64[ns]
  * categorical → string (cleaned, uppercase)
* Ensure consistency across entire column

---
## STEP 8: SAFE GUARD
* Ensure row count remains same unless rows are completely invalid
* Ensure no column is left with mixed inconsistent types

---
CURRENT ANOMALY TARGET:
  Column: {col}
  Check type: {anomaly['check_type']}
  Detail: {anomaly['detail']}

COLUMN PROFILE:
  dtype: {col_stats.get('dtype', 'unknown')}
  null_rate: {col_stats.get('null_rate', 'N/A')}
  sample_values: {col_stats.get('sample_values', [])}

DATASET OVERVIEW: row_count={profile['row_count']}
{prev_context}

---
## OUTPUT REQUIREMENT:
Return ONLY a JSON object with these exact keys (no markdown outside JSON). The "fix_code" value must contain the python pandas code derived from your strictly enforced rules above.
{{
  "diagnosis": "one sentence root cause",
  "fix_code": "Executable python pandas code operating on dataframe `df`. Must mutate inline.",
  "confidence": "HIGH or MEDIUM or LOW",
  "recommended_action": "What a human engineer should do if automated fix fails"
}}"""
    return prompt


def call_claude(anomaly: dict, profile: dict, attempt: int = 1, previous_fix: str = None) -> dict:
    """
    Backward-compatible function name used by healer.py.
    Calls OpenAI and returns parsed response dict.
    """
    if not OPENAI_API_KEY:
        raise RuntimeError(
            "OPENAI_API_KEY is missing. Add it to your .env file before running self-healing."
        )

    prompt = build_prompt(anomaly, profile, attempt, previous_fix)

    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You are a strict JSON generator. Return valid JSON only.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": MAX_TOKENS,
    }

    response = requests.post(
        OPENAI_API_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
        json=payload,
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    raw_text = data["choices"][0]["message"]["content"]

    clean = raw_text.strip()
    if clean.startswith("```"):
        clean = clean.split("```")[1]
        if clean.startswith("json"):
            clean = clean[4:]
    clean = clean.strip().rstrip("```").strip()

    result = json.loads(clean)

    for key in ("diagnosis", "fix_code", "confidence", "recommended_action"):
        if key not in result:
            result[key] = "N/A"

    return result
