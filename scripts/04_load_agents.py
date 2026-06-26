import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import uuid
import random
from datetime import datetime
from config.db import engine
from config.gcp import dump_df_to_gcs

counties = ["Nairobi", "Kiambu", "Mombasa", "Nakuru", "Kisumu"]

agents = []

for _ in range(50):
    agents.append({
        "agent_id": str(uuid.uuid4()),
        "full_name": f"Agent {uuid.uuid4().hex[:6]}",
        "phone": f"07{random.randint(10000000,99999999)}",
        "email": f"agent{random.randint(1,9999)}@insurance.co.ke",
        "county": random.choice(counties),
        "hire_date": datetime.now().date(),
        "status": "Active"
    })

df = pd.DataFrame(agents)

dump_df_to_gcs(df, "agents")
df.to_sql("agents", engine, if_exists="append", index=False, method="multi")
print("Agents loaded")
