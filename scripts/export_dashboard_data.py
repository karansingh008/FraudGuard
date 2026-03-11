"""
Export Dashboard Data — Credit Card Dataset
=============================================
Queries MySQL and writes JSON files for the web dashboard.
Handles NaN → null conversion for valid JSON output.
"""

import json
import math
import os
import sys
import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_connection_string

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard", "data")


def _clean_for_json(obj):
    """Replace NaN / Inf with None for valid JSON."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def _write(name, data):
    path = os.path.join(OUTPUT_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"   📄 {name}")


def _df_to_records(df):
    """Convert DataFrame to list of dicts with NaN replaced by None."""
    records = df.to_dict(orient="records")
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                rec[k] = None
    return records


def main():
    print("━" * 60)
    print("  EXPORT DASHBOARD DATA")
    print("━" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = create_engine(get_connection_string())

    # 1. Quality history
    qh = pd.read_sql("SELECT * FROM data_quality_history ORDER BY record_date", engine)
    _write("quality_history.json", _df_to_records(qh))

    # 2. Validation summary (count per rule)
    vs = pd.read_sql("""
        SELECT rule_name,
               SUM(rule_passed = 1)  AS passed,
               SUM(rule_passed = 0)  AS failed
        FROM validation_results
        GROUP BY rule_name
    """, engine)
    _write("validation_summary.json", _df_to_records(vs))

    # 3. Individual validation errors (only failed, limit to 500 for dashboard)
    ve = pd.read_sql("""
        SELECT vr.transaction_id, vr.rule_name, rt.amount
        FROM validation_results vr
        LEFT JOIN raw_transactions rt ON vr.transaction_id = rt.transaction_id
        WHERE vr.rule_passed = 0
        LIMIT 500
    """, engine)
    _write("validation_errors.json", _df_to_records(ve))

    # 4. Anomalies (top 200 by score)
    an = pd.read_sql("""
        SELECT a.transaction_id, a.anomaly_score, a.detection_method,
               rt.amount, rt.class AS is_fraud
        FROM anomalies a
        LEFT JOIN raw_transactions rt ON a.transaction_id = rt.transaction_id
        ORDER BY a.anomaly_score ASC
        LIMIT 200
    """, engine)
    _write("anomalies.json", _df_to_records(an))

    # 5. Overview stats
    total_rows = pd.read_sql("SELECT COUNT(*) AS cnt FROM raw_transactions", engine)["cnt"].iloc[0]
    total_checks = pd.read_sql("SELECT COUNT(*) AS cnt FROM validation_results", engine)["cnt"].iloc[0]
    failed_checks = pd.read_sql("SELECT COUNT(*) AS cnt FROM validation_results WHERE rule_passed = 0", engine)["cnt"].iloc[0]
    anomaly_count = pd.read_sql("SELECT COUNT(*) AS cnt FROM anomalies", engine)["cnt"].iloc[0]
    total_frauds = pd.read_sql("SELECT COUNT(*) AS cnt FROM raw_transactions WHERE class = 1", engine)["cnt"].iloc[0]
    frauds_detected = pd.read_sql("""
        SELECT COUNT(*) AS cnt FROM anomalies a
        JOIN raw_transactions rt ON a.transaction_id = rt.transaction_id
        WHERE rt.class = 1
    """, engine)["cnt"].iloc[0]

    latest_score = None
    if not qh.empty:
        latest_score = float(qh.iloc[-1]["quality_score"])

    overview = {
        "total_records": int(total_rows),
        "total_checks": int(total_checks),
        "failed_checks": int(failed_checks),
        "passed_checks": int(total_checks - failed_checks),
        "quality_score": latest_score,
        "anomaly_count": int(anomaly_count),
        "total_frauds": int(total_frauds),
        "frauds_detected": int(frauds_detected),
    }
    _write("overview.json", overview)

    # 6. Fraud detection breakdown
    fraud_stats = pd.read_sql("""
        SELECT
            rt.class AS is_fraud,
            CASE WHEN a.id IS NOT NULL THEN 'Flagged' ELSE 'Not Flagged' END AS status,
            COUNT(*) AS cnt
        FROM raw_transactions rt
        LEFT JOIN anomalies a ON rt.transaction_id = a.transaction_id
        GROUP BY rt.class, status
    """, engine)
    _write("fraud_detection_stats.json", _df_to_records(fraud_stats))

    # 7. Amount distribution of anomalies vs normal
    amount_dist = pd.read_sql("""
        SELECT
            CASE
                WHEN rt.amount < 10 THEN '$0-10'
                WHEN rt.amount < 50 THEN '$10-50'
                WHEN rt.amount < 100 THEN '$50-100'
                WHEN rt.amount < 500 THEN '$100-500'
                WHEN rt.amount < 1000 THEN '$500-1K'
                WHEN rt.amount < 5000 THEN '$1K-5K'
                ELSE '$5K+'
            END AS amount_range,
            COUNT(*) AS total,
            SUM(CASE WHEN a.id IS NOT NULL THEN 1 ELSE 0 END) AS anomalies
        FROM raw_transactions rt
        LEFT JOIN anomalies a ON rt.transaction_id = a.transaction_id
        GROUP BY amount_range
        ORDER BY MIN(rt.amount)
    """, engine)
    _write("amount_distribution.json", _df_to_records(amount_dist))

    print(f"\n✅  Exported dashboard data to {os.path.abspath(OUTPUT_DIR)}\n")


if __name__ == "__main__":
    main()
