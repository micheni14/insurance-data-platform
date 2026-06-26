import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import random
from datetime import datetime, timedelta
import uuid
from config.db import engine
from config.gcp import dump_df_to_gcs

print("Connected to database")

customers = pd.read_sql("SELECT customer_id FROM customers", engine)

if customers.empty:
    raise Exception("No customers found. Run 01_generate_customers.py first.")

print(f"Found {len(customers)} customers")

policy_types = ["Life", "Health", "Auto", "Education"]

def generate_policy(customer_id):
    start_date = datetime.today() - timedelta(days=random.randint(0, 1000))
    end_date = start_date + timedelta(days=365 * 5)

    return {
        "policy_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "policy_type": random.choice(policy_types),
        "premium": round(random.uniform(5000, 50000), 2),
        "start_date": start_date.date(),
        "end_date": end_date.date(),
        "status": "Active"
    }


policies = [
    generate_policy(row["customer_id"])
    for _, row in customers.iterrows()
]

df = pd.DataFrame(policies)

print(f"Policies to insert: {len(df)}")

dump_df_to_gcs(df, "policies")
df.to_sql(
    "policies",
    engine,
    if_exists="append",
    index=False,
    chunksize=500,
    method="multi"
)

print("Policies inserted successfully")
