"""
profiler.py — Step 1 of the agent loop.
Loads the CSV dataset and computes a statistical profile per column:
  - null_rate, dtype, min, max, mean, unique_count, row_count
Returns a dict usable by detector.py and healer.py.
"""

import pandas as pd
import numpy as np
import os

DATA_PATH = os.environ.get("DATA_PATH", "data/sales.csv")


def load_and_profile(path: str = DATA_PATH) -> dict:
    """Load CSV and return a profile dict + the raw DataFrame."""
    df = pd.read_csv(path)
    profile = {
        "row_count": len(df),
        "columns": {},
    }

    for col in df.columns:
        series = df[col]
        col_profile = {
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "null_rate": round(float(series.isna().mean()), 4),
            "unique_count": int(series.nunique()),
        }

        # Numeric stats
        if pd.api.types.is_numeric_dtype(series):
            col_profile["min"] = float(series.min()) if not series.isna().all() else None
            col_profile["max"] = float(series.max()) if not series.isna().all() else None
            col_profile["mean"] = float(series.mean()) if not series.isna().all() else None
            col_profile["std"] = float(series.std()) if not series.isna().all() else None
        else:
            col_profile["min"] = None
            col_profile["max"] = None
            col_profile["mean"] = None
            col_profile["std"] = None

        # Sample values (for context sent to Claude)
        sample = series.dropna().head(5).tolist()
        col_profile["sample_values"] = [str(v) for v in sample]

        profile["columns"][col] = col_profile

    return profile, df


if __name__ == "__main__":
    profile, df = load_and_profile()
    print(f"Rows: {profile['row_count']}")
    for col, stats in profile["columns"].items():
        print(f"  {col}: null_rate={stats['null_rate']}, dtype={stats['dtype']}, unique={stats['unique_count']}")
