import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import uuid
import random
from datetime import datetime
from config.db import engine

policies = pd.read_sql("SELECT policy_id, customer_id, premium FROM policies", engine)
agents = pd.read_sql("SELECT agent_id FROM agents", engine)

sales = []

for _, row in policies.iterrows():
    agent = agents.sample(1).iloc[0]
    commission_rate = random.uniform(0.05, 0.15)
    sales.append({
        "sale_id": str(uuid.uuid4()),
        "policy_id": row["policy_id"],
        "customer_id": row["customer_id"],
        "agent_id": agent["agent_id"],
        "sale_date": datetime.now().date(),
        "commission_rate": round(commission_rate, 2),
        "commission_amount": round(row["premium"] * commission_rate, 2),
        "premium_amount": row["premium"]
    })

df = pd.DataFrame(sales)

df.to_sql("sales", engine, if_exists="append", index=False, method="multi")
print("Sales loaded")
