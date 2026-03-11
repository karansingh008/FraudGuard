"""
Anomaly Detection Script — Credit Card Dataset
================================================
Loads transactions from `raw_transactions`, trains an Isolation Forest
on Amount + selected PCA features, stores flagged anomalies in `anomalies`.
"""

import os
import sys
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_connection_string


def main():
    print("━" * 60)
    print("  ANOMALY DETECTION — Credit Card (Isolation Forest)")
    print("━" * 60)

    engine = create_engine(get_connection_string())

    # Load a sample for training (full 284k is fine for IF but let's be safe)
    df = pd.read_sql("""
        SELECT transaction_id, amount, v1, v2, v3, v4, v5, v6, v7,
               v14, v17, time_elapsed, class
        FROM raw_transactions
    """, engine)
    print(f"📂  Loaded {len(df)} transactions for analysis.")

    if df.empty:
        print("⚠️  No data. Run validate_data.py first.")
        return

    # Features for anomaly detection
    feature_cols = ["amount", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v14", "v17"]
    X = df[feature_cols].values

    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train Isolation Forest
    print("🌲  Training Isolation Forest...")
    model = IsolationForest(
        n_estimators=100,
        contamination=0.02,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    predictions = model.predict(X_scaled)
    scores = model.decision_function(X_scaled)

    df["prediction"] = predictions
    df["anomaly_score"] = np.round(scores, 6)

    anomalies = df[df["prediction"] == -1].copy()
    normals = df[df["prediction"] == 1]

    # Check how many known frauds were caught
    fraud_caught = anomalies[anomalies["class"] == 1]
    total_frauds = (df["class"] == 1).sum()

    print(f"\n📊  Results:")
    print(f"   Normal          : {len(normals)}")
    print(f"   Anomaly (ML)    : {len(anomalies)}")
    print(f"   Known frauds    : {total_frauds}")
    print(f"   Frauds caught   : {len(fraud_caught)}  ({100 * len(fraud_caught) / max(total_frauds, 1):.1f}%)")

    if not anomalies.empty:
        print(f"\n   Top anomalies by score:")
        top = anomalies.nsmallest(10, "anomaly_score")
        for _, r in top.iterrows():
            label = "FRAUD" if r["class"] == 1 else "Normal"
            print(f"     • TX {int(r['transaction_id'])}: ${r['amount']:,.2f}  score={r['anomaly_score']:.4f}  [{label}]")

    # Store in MySQL
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM anomalies"))
        conn.commit()

    anomaly_records = anomalies[["transaction_id", "anomaly_score"]].copy()
    anomaly_records["detection_method"] = "IsolationForest"

    chunk_size = 5000
    for start in range(0, len(anomaly_records), chunk_size):
        chunk = anomaly_records.iloc[start:start + chunk_size]
        chunk.to_sql("anomalies", engine, if_exists="append", index=False)

    print(f"\n💾  Stored {len(anomaly_records)} anomalies in MySQL.")
    print("✅  Anomaly detection complete.\n")


if __name__ == "__main__":
    main()
