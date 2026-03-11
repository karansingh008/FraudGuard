"""
Generate a realistic sample_transactions.csv with ~1000 rows.

Intentionally injects:
  - ~2 % negative / zero amounts
  - ~1 % NULL customer IDs
  - ~1 % invalid dates (stored as empty string)
  - ~1 % duplicate transaction IDs
  - ~2 % anomaly-level high amounts ($8 000 – $15 000)

The remaining ~93 % are clean, normal transactions ($10 – $500).
"""

import csv
import os
import random
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "data", "sample_transactions.csv")

REGIONS = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad",
           "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow"]

NUM_ROWS = 1000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)


def random_date():
    delta = (END_DATE - START_DATE).days
    return (START_DATE + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def generate():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    rows = []
    used_ids = set()

    for i in range(1, NUM_ROWS + 1):
        tid = 1000 + i
        date = random_date()
        cid = random.randint(100, 999)
        amount = round(random.uniform(10, 500), 2)
        region = random.choice(REGIONS)

        rand = random.random()

        if rand < 0.02:
            # negative / zero amount
            amount = round(random.uniform(-100, 0), 2)
        elif rand < 0.03:
            # null customer ID
            cid = ""
        elif rand < 0.04:
            # invalid date
            date = ""
        elif rand < 0.05:
            # duplicate transaction ID  (copy an earlier one)
            if rows:
                tid = random.choice(rows)["transaction_id"]
        elif rand < 0.07:
            # anomaly-level amount
            amount = round(random.uniform(8000, 15000), 2)

        used_ids.add(tid)
        rows.append({
            "transaction_id": tid,
            "transaction_date": date,
            "customer_id": cid,
            "amount": amount,
            "region": region,
        })

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "transaction_id", "transaction_date", "customer_id", "amount", "region"
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅  Generated {len(rows)} rows → {os.path.abspath(OUTPUT_PATH)}")


if __name__ == "__main__":
    generate()
