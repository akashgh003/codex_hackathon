"""
seed_data.py — Generate a realistic sales CSV for demo.
Run once before starting the agent.
"""

import pandas as pd
import numpy as np
import os

DATA_PATH = os.environ.get("DATA_PATH", "data/sales.csv")
os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)

np.random.seed(42)
N = 1000

df = pd.DataFrame({
    "order_id":     range(1000, 1000 + N),
    "product":      np.random.choice(["Widget A", "Widget B", "Widget C", "Gadget X"], N),
    "price":        np.round(np.random.uniform(9.99, 499.99, N), 2),
    "quantity":     np.random.randint(1, 50, N),
    "revenue":      None,  # will compute
    "region":       np.random.choice(["North", "South", "East", "West"], N),
    "customer_age": np.random.randint(18, 75, N),
    "discount":     np.round(np.random.uniform(0.0, 0.30, N), 2),
})
df["revenue"] = np.round(df["price"] * df["quantity"] * (1 - df["discount"]), 2)

df.to_csv(DATA_PATH, index=False)
print(f"Seeded {N} rows -> {DATA_PATH}")
print(df.describe())
