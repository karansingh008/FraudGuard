"""
Data Validation Script — Credit Card Transaction Dataset
=========================================================
Loads DataSet/creditcard.csv, applies validation rules, inserts
raw data into `raw_transactions` and results into `validation_results`.

Validation rules
----------------
1. Amount must be non-negative  (amount >= 0)
2. Time must be non-negative    (time >= 0)
3. No null / NaN values in any feature column
4. Class must be 0 or 1
5. Amount must not be extreme outlier (> $20,000)
"""

import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_connection_string, get_server_connection_string

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "DataSet", "creditcard.csv")


# ────────────────────────────────────────────────────────────────
# 0.  Ensure database + tables exist
# ────────────────────────────────────────────────────────────────
def _bootstrap_database():
    """Create the analytics DB and tables if they don't exist."""
    sql_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql", "create_tables.sql")
    server_engine = create_engine(get_server_connection_string())

    with server_engine.connect() as conn:
        conn.execute(text("CREATE DATABASE IF NOT EXISTS analytics"))
        conn.commit()

    engine = create_engine(get_connection_string())
    with open(sql_file, "r") as f:
        sql_script = f.read()

    skip_prefixes = ("create database", "use ")

    with engine.connect() as conn:
        for statement in sql_script.split(";"):
            lines = [l for l in statement.splitlines() if not l.strip().startswith("--")]
            stmt = "\n".join(lines).strip()
            if not stmt:
                continue
            if stmt.lower().startswith(skip_prefixes):
                continue
            try:
                conn.execute(text(stmt))
            except Exception:
                pass
        conn.commit()

    return engine


# ────────────────────────────────────────────────────────────────
# 1.  Validation
# ────────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply validation rules and return a DataFrame of results.
    Columns: transaction_id, rule_name, rule_passed
    """
    results = []

    # Vectorised checks for speed (284k rows)
    # Rule 1 – Non-negative amount
    amt_ok = df["Amount"] >= 0
    # Rule 2 – Non-negative time
    time_ok = df["Time"] >= 0
    # Rule 3 – No nulls in any column
    null_ok = ~df.isnull().any(axis=1)
    # Rule 4 – Class must be 0 or 1
    class_ok = df["Class"].isin([0, 1])
    # Rule 5 – Amount not extreme outlier (> $20,000)
    amt_reasonable = df["Amount"] <= 20000

    for idx, row in df.iterrows():
        tid = idx + 1  # 1-based transaction_id
        results.append({"transaction_id": tid, "rule_name": "non_negative_amount", "rule_passed": bool(amt_ok.iloc[idx] if isinstance(idx, int) else amt_ok.loc[idx])})
        results.append({"transaction_id": tid, "rule_name": "non_negative_time", "rule_passed": bool(time_ok.iloc[idx] if isinstance(idx, int) else time_ok.loc[idx])})
        results.append({"transaction_id": tid, "rule_name": "no_null_values", "rule_passed": bool(null_ok.iloc[idx] if isinstance(idx, int) else null_ok.loc[idx])})
        results.append({"transaction_id": tid, "rule_name": "valid_class_label", "rule_passed": bool(class_ok.iloc[idx] if isinstance(idx, int) else class_ok.loc[idx])})
        results.append({"transaction_id": tid, "rule_name": "reasonable_amount", "rule_passed": bool(amt_reasonable.iloc[idx] if isinstance(idx, int) else amt_reasonable.loc[idx])})

    return pd.DataFrame(results)


# ────────────────────────────────────────────────────────────────
# 2.  Main pipeline
# ────────────────────────────────────────────────────────────────
def main():
    print("━" * 60)
    print("  DATA VALIDATION PIPELINE — Credit Card Dataset")
    print("━" * 60)

    engine = _bootstrap_database()
    print("✅  Database & tables ready.")

    # Load CSV
    df = pd.read_csv(CSV_PATH)
    print(f"📂  Loaded {len(df)} rows from creditcard.csv")

    # Prepare for MySQL insert — rename columns to match schema
    db_df = df.copy()
    col_map = {"Time": "time_elapsed", "Class": "class"}
    for i in range(1, 29):
        col_map[f"V{i}"] = f"v{i}"
    col_map["Amount"] = "amount"
    db_df.rename(columns=col_map, inplace=True)

    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM anomalies"))
        conn.execute(text("DELETE FROM validation_results"))
        conn.execute(text("DELETE FROM raw_transactions"))
        conn.commit()
    print("🗑️  Cleared existing data.")

    # Insert in chunks (284k rows)
    chunk_size = 10000
    total = len(db_df)
    for start in range(0, total, chunk_size):
        chunk = db_df.iloc[start:start + chunk_size]
        chunk.to_sql("raw_transactions", engine, if_exists="append", index=False)
        pct = min(100, int((start + chunk_size) / total * 100))
        print(f"   💾  Inserted rows {start + 1}–{min(start + chunk_size, total)} ({pct}%)")

    print(f"💾  Inserted {total} rows into raw_transactions.")

    # Validate (vectorised computation, then build results)
    print("🔍  Running validation checks (this may take a moment)...")

    # For performance with 284k rows, use vectorised approach
    amt_ok = (df["Amount"] >= 0).values
    time_ok = (df["Time"] >= 0).values
    null_ok = (~df.isnull().any(axis=1)).values
    class_ok = df["Class"].isin([0, 1]).values
    amt_reasonable = (df["Amount"] <= 20000).values

    results = []
    for i in range(len(df)):
        tid = i + 1
        results.append({"transaction_id": tid, "rule_name": "non_negative_amount", "rule_passed": bool(amt_ok[i])})
        results.append({"transaction_id": tid, "rule_name": "non_negative_time", "rule_passed": bool(time_ok[i])})
        results.append({"transaction_id": tid, "rule_name": "no_null_values", "rule_passed": bool(null_ok[i])})
        results.append({"transaction_id": tid, "rule_name": "valid_class_label", "rule_passed": bool(class_ok[i])})
        results.append({"transaction_id": tid, "rule_name": "reasonable_amount", "rule_passed": bool(amt_reasonable[i])})

    val_df = pd.DataFrame(results)
    print(f"🔍  Ran {len(val_df)} validation checks.")

    # Insert validation results in chunks
    for start in range(0, len(val_df), chunk_size):
        chunk = val_df.iloc[start:start + chunk_size]
        chunk.to_sql("validation_results", engine, if_exists="append", index=False)

    # Summary
    failed = val_df[val_df["rule_passed"] == False]
    print(f"\n📊  Summary:")
    print(f"   Total checks  : {len(val_df)}")
    print(f"   Passed        : {len(val_df) - len(failed)}")
    print(f"   Failed        : {len(failed)}")
    if not failed.empty:
        print(f"\n   Failures by rule:")
        for rule, count in failed.groupby("rule_name").size().items():
            print(f"     • {rule}: {count}")

    print("\n✅  Validation complete.\n")


if __name__ == "__main__":
    main()
