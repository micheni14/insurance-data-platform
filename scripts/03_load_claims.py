import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
from config.db import engine
from config.gcp import dump_df_to_gcs

print("Connected to DB")

policies = pd.read_sql("""
    SELECT policy_id, customer_id, start_date, end_date
    FROM policies
""", engine)

if policies.empty:
    raise Exception("No policies found. Load policies first.")

print(f"Found {len(policies)} policies")

claim_types = ["Medical", "Accident", "Theft", "Fire", "Death"]
statuses = ["Pending", "Approved", "Rejected", "Paid"]

claims = []

for _, row in policies.iterrows():
    if random.random() < 0.4:
        incident_date = row["start_date"] + timedelta(days=random.randint(30, 1500))
        claim_amount = round(random.uniform(5000, 200000), 2)
        status = random.choice(statuses)
        approved_amount = (
            round(claim_amount * random.uniform(0.3, 1.0), 2)
            if status in ["Approved", "Paid"]
            else 0
        )
        claims.append({
            "claim_id": str(uuid.uuid4()),
            "policy_id": row["policy_id"],
            "customer_id": row["customer_id"],
            "claim_type": random.choice(claim_types),
            "claim_amount": claim_amount,
            "claim_status": status,
            "incident_date": incident_date,
            "claim_date": incident_date + timedelta(days=random.randint(1, 30)),
            "approved_amount": approved_amount,
            "created_at": datetime.now()
        })

df = pd.DataFrame(claims)

print(f"Generated {len(df)} claims")

dump_df_to_gcs(df, "claims")
df.to_sql(
    "claims",
    engine,
    if_exists="append",
    index=False,
    chunksize=500,
    method="multi"
)

print("Claims loaded successfully")
