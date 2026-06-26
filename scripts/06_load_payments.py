import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import uuid
import random
from datetime import datetime, timedelta
from config.db import engine
from config.gcp import dump_df_to_gcs

policies = pd.read_sql("SELECT policy_id, customer_id, premium FROM policies", engine)

payments = []
methods = ["M-Pesa", "Bank", "Card"]

for _, row in policies.iterrows():
    for i in range(random.randint(1, 6)):
        payments.append({
            "payment_id": str(uuid.uuid4()),
            "policy_id": row["policy_id"],
            "customer_id": row["customer_id"],
            "payment_date": datetime.now().date() - timedelta(days=random.randint(0, 180)),
            "amount": round(row["premium"] / 6, 2),
            "payment_method": random.choice(methods),
            "status": "Completed"
        })

df = pd.DataFrame(payments)

dump_df_to_gcs(df, "payments")
df.to_sql("payments", engine, if_exists="append", index=False, method="multi")
print("Payments loaded")
