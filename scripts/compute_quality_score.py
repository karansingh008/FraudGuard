"""
Compute Data Quality Score
==========================
Queries `validation_results` to compute:

    quality_score = 100 × (1 − failed_checks / total_checks)

Inserts the result into `data_quality_history` with today's date.
"""

import os
import sys
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_connection_string


def main():
    print("━" * 60)
    print("  DATA QUALITY SCORE COMPUTATION")
    print("━" * 60)

    engine = create_engine(get_connection_string())

    # Fetch validation results
    query = "SELECT rule_passed FROM validation_results"
    df = pd.read_sql(query, engine)

    total_checks = len(df)
    failed_checks = int((df["rule_passed"] == 0).sum())
    passed_checks = total_checks - failed_checks

    if total_checks == 0:
        print("⚠️  No validation results found. Run validate_data.py first.")
        return

    quality_score = round(100 * (1 - failed_checks / total_checks), 2)

    # Get total unique rows
    row_count = pd.read_sql("SELECT COUNT(*) AS cnt FROM raw_transactions", engine)["cnt"].iloc[0]

    print(f"\n📊  Quality Report:")
    print(f"   Date           : {date.today()}")
    print(f"   Total records  : {row_count}")
    print(f"   Total checks   : {total_checks}")
    print(f"   Passed checks  : {passed_checks}")
    print(f"   Failed checks  : {failed_checks}")
    print(f"   Quality Score  : {quality_score}%")

    # Insert into history
    history = pd.DataFrame([{
        "record_date": date.today(),
        "quality_score": quality_score,
        "total_rows": int(row_count),
        "failed_checks": failed_checks,
    }])
    history.to_sql("data_quality_history", engine, if_exists="append", index=False)
    print(f"\n💾  Saved to data_quality_history table.")
    print("✅  Quality score computation complete.\n")


if __name__ == "__main__":
    main()
