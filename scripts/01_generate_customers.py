import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from faker import Faker
import pandas as pd
import uuid
from datetime import datetime
from config.db import engine
from config.gcp import dump_df_to_gcs

fake = Faker()

print("Generating customers...")

data = []

for _ in range(2000):
    data.append({
        "customer_id": str(uuid.uuid4()),
        "full_name": fake.name(),
        "age": fake.random_int(18, 80),
        "gender": fake.random_element(["Male", "Female"]),
        "county": fake.random_element(["Nairobi", "Kiambu", "Mombasa", "Nakuru", "Kisumu"]),
        "signup_date": fake.date_between(start_date="-5y", end_date="today"),
        "ingested_at": datetime.now()
    })

df = pd.DataFrame(data)

print("DataFrame shape:", df.shape)

print("Checking existing customers...")

existing_ids = pd.read_sql("SELECT customer_id FROM customers", engine)

df = df[~df["customer_id"].isin(existing_ids["customer_id"])]

print(f"New records after dedup: {len(df)}")

print("Loading into Postgres...")

dump_df_to_gcs(df, "customers")
df.to_sql(
    "customers",
    engine,
    if_exists="append",
    index=False,
    chunksize=500,
    method="multi"
)

print("Customers loaded successfully")
