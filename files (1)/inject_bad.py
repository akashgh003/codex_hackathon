"""
inject_bad.py — Demo script.
Injects realistic data quality issues into the CSV so the agent
can detect and self-heal them live during the demo.

Issues injected (randomly chosen each run):
  - NULL RATE:       Set 40% of a numeric column to NaN
  - TYPE DRIFT:      Corrupt a numeric column with string values
  - DUPLICATES:      Duplicate 200 random rows
  - RANGE VIOLATION: Set random rows to 100x the normal max

Usage:
  python inject_bad.py [--scenario null|type|dupe|range]
"""

import pandas as pd
import numpy as np
import os
import sys
import random
import argparse

DATA_PATH = os.environ.get("DATA_PATH", "data/sales.csv")


def inject_null_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Set 40% of a random numeric column to NaN."""
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        print("No numeric columns to corrupt.")
        return df
    col = random.choice(num_cols)
    mask = np.random.rand(len(df)) < 0.40
    df.loc[mask, col] = np.nan
    print(f"[INJECT] null_rate -> set 40% of '{col}' to NaN")
    return df


def inject_type_drift(df: pd.DataFrame) -> pd.DataFrame:
    """Corrupt a numeric column by inserting string values."""
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        print("No numeric columns to corrupt.")
        return df
    col = random.choice(num_cols)
    idx = df.sample(frac=0.30).index
    df[col] = df[col].astype(object)
    df.loc[idx, col] = "CORRUPT_STRING"
    print(f"[INJECT] type_drift -> corrupted 30% of '{col}' with strings")
    return df


def inject_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Duplicate 200 random rows (or 20% of df, whichever is smaller)."""
    n = min(200, int(len(df) * 0.20))
    dupes = df.sample(n=n, replace=True)
    df = pd.concat([df, dupes], ignore_index=True)
    print(f"[INJECT] duplicates -> added {n} duplicate rows (total: {len(df)})")
    return df


def inject_range_violation(df: pd.DataFrame) -> pd.DataFrame:
    """Set random rows of a numeric column to 100x the column max."""
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        print("No numeric columns to corrupt.")
        return df
    col = random.choice(num_cols)
    idx = df.sample(frac=0.05).index
    extreme_value = df[col].max() * 100
    df.loc[idx, col] = extreme_value
    print(f"[INJECT] range_violation -> set 5% of '{col}' to {extreme_value:.2f}")
    return df


SCENARIOS = {
    "null": inject_null_rate,
    "type": inject_type_drift,
    "dupe": inject_duplicates,
    "range": inject_range_violation,
}


def main():
    parser = argparse.ArgumentParser(description="Inject bad data for demo.")
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()),
        default=None,
        help="Which scenario to inject (default: random)",
    )
    args = parser.parse_args()

    # Load
    if not os.path.exists(DATA_PATH):
        print(f"Data file not found: {DATA_PATH}")
        sys.exit(1)

    df = pd.read_csv(DATA_PATH)
    print(f"Loaded {len(df)} rows from {DATA_PATH}")

    # Choose scenario
    scenario_key = args.scenario or random.choice(list(SCENARIOS.keys()))
    print(f"Running scenario: {scenario_key}")
    df = SCENARIOS[scenario_key](df)

    # Save
    df.to_csv(DATA_PATH, index=False)
    print(f"Saved corrupted data -> {DATA_PATH}")
    print("The agent will detect and heal this in the next poll cycle (~30s).")


if __name__ == "__main__":
    main()
