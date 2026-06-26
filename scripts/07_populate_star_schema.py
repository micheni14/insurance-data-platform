import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import random
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import text
from config.db import engine

print("Starting star schema dimension pipeline...\n")

# ============================================================
# DIMENSION 1: dim_date
# ============================================================
print("Populating dim_date...")

start = date(2019, 1, 1)
end   = date(2030, 12, 31)
dates = []

current = start
while current <= end:
    dates.append({
        "date_key":       current,
        "day":            current.day,
        "month":          current.month,
        "month_name":     current.strftime("%B"),
        "quarter":        (current.month - 1) // 3 + 1,
        "quarter_name":   f"Q{(current.month - 1) // 3 + 1} {current.year}",
        "year":           current.year,
        "day_of_week":    current.isoweekday(),
        "day_name":       current.strftime("%A"),
        "is_weekend":     current.isoweekday() >= 6,
        "is_month_start": current.day == 1,
        "is_month_end":   (current + timedelta(days=1)).month != current.month,
        "week_of_year":   current.isocalendar()[1]
    })
    current += timedelta(days=1)

df_date = pd.DataFrame(dates)

try:
    existing_dates = pd.read_sql("SELECT date_key FROM dim_date", engine)
except Exception:
    existing_dates = pd.DataFrame()
if not existing_dates.empty:
    df_date = df_date[~df_date["date_key"].isin(existing_dates["date_key"])]

if not df_date.empty:
    df_date.to_sql("dim_date", engine, if_exists="append", index=False, chunksize=100, method="multi")
    print(f"  dim_date loaded: {len(df_date)} rows")
else:
    print("  dim_date already populated")


# ============================================================
# DIMENSION 2: dim_policy_type
# ============================================================
print("\nPopulating dim_policy_type...")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_policy_type (
            policy_type_id SERIAL PRIMARY KEY,
            policy_type    VARCHAR(50) UNIQUE NOT NULL
        )
    """))

existing_types = pd.read_sql("SELECT policy_type FROM dim_policy_type", engine)
source_types = pd.read_sql("SELECT DISTINCT policy_type FROM policies", engine)
new_types = source_types[~source_types["policy_type"].isin(existing_types["policy_type"])]

if not new_types.empty:
    new_types.to_sql("dim_policy_type", engine, if_exists="append", index=False, method="multi")
    print(f"  dim_policy_type loaded: {len(new_types)} rows")
else:
    print("  dim_policy_type already populated")


# ============================================================
# DIMENSION 3: dim_county
# ============================================================
print("\nPopulating dim_county...")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_county (
            county_id   SERIAL PRIMARY KEY,
            county_name VARCHAR(100) UNIQUE NOT NULL
        )
    """))

existing_counties = pd.read_sql("SELECT county_name FROM dim_county", engine)
source_counties = pd.read_sql("SELECT DISTINCT county FROM customers", engine)
new_counties = source_counties[~source_counties["county"].isin(existing_counties["county_name"])]
new_counties = new_counties.rename(columns={"county": "county_name"})

if not new_counties.empty:
    new_counties.to_sql("dim_county", engine, if_exists="append", index=False, method="multi")
    print(f"  dim_county loaded: {len(new_counties)} rows")
else:
    print("  dim_county already populated")


# ============================================================
# DIMENSION 4: dim_claim_status
# ============================================================
print("\nPopulating dim_claim_status...")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_claim_status (
            status_id   SERIAL PRIMARY KEY,
            status_name VARCHAR(50) UNIQUE NOT NULL
        )
    """))

existing_statuses = pd.read_sql("SELECT status_name FROM dim_claim_status", engine)
default_statuses = pd.DataFrame({
    "status_name": ["Pending", "Approved", "Rejected", "Paid", "Escalated"]
})
new_statuses = default_statuses[~default_statuses["status_name"].isin(existing_statuses["status_name"])]

if not new_statuses.empty:
    new_statuses.to_sql("dim_claim_status", engine, if_exists="append", index=False, method="multi")
    print(f"  dim_claim_status loaded: {len(new_statuses)} rows")
else:
    print("  dim_claim_status already populated")


# ============================================================
# DIMENSION 5: dim_customer
# ============================================================
print("\nPopulating dim_customer...")

customers = pd.read_sql("SELECT * FROM customers", engine)

def get_age_band(age):
    if age <= 25:   return "18-25"
    elif age <= 35: return "26-35"
    elif age <= 45: return "36-45"
    elif age <= 55: return "46-55"
    elif age <= 65: return "56-65"
    else:           return "66+"

def get_region(county):
    regions = {
        "Nairobi": "Nairobi Metro",
        "Kiambu":  "Central",
        "Mombasa": "Coast",
        "Nakuru":  "Rift Valley",
        "Kisumu":  "Nyanza"
    }
    return regions.get(county, "Other")

def get_tenure(signup_date):
    if pd.isna(signup_date): return "Unknown"
    days = (datetime.today().date() - pd.to_datetime(signup_date).date()).days
    if days < 365:   return "New"
    elif days < 730: return "Established"
    else:            return "Loyal"

customers["age_band"]        = customers["age"].apply(get_age_band)
customers["region"]          = customers["county"].apply(get_region)
customers["customer_tenure"] = customers["signup_date"].apply(get_tenure)

dim_customer = customers[[
    "customer_id", "full_name", "age", "age_band",
    "gender", "county", "region", "signup_date",
    "customer_tenure", "ingested_at"
]]

try:
    existing_customers = pd.read_sql("SELECT customer_id FROM dim_customer", engine)
except Exception:
    existing_customers = pd.DataFrame()
if not existing_customers.empty:
    dim_customer = dim_customer[~dim_customer["customer_id"].astype(str).isin(existing_customers["customer_id"].astype(str))]
if not dim_customer.empty:
    dim_customer.to_sql("dim_customer", engine, if_exists="append", index=False, chunksize=100, method="multi")
    print(f"  dim_customer loaded: {len(dim_customer)} rows")
else:
    print("  dim_customer already populated")


# ============================================================
# DIMENSION 6: dim_agent (populated from agents table)
# ============================================================
print("\nPopulating dim_agent...")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_agent (
            agent_id   VARCHAR(36) PRIMARY KEY,
            agent_name VARCHAR(100),
            agent_code VARCHAR(20),
            region     VARCHAR(50),
            channel    VARCHAR(50),
            hire_date  DATE,
            is_active  BOOLEAN DEFAULT TRUE
        )
    """))

regions_map = {
    "Nairobi": "Nairobi Metro",
    "Kiambu":  "Central",
    "Mombasa": "Coast",
    "Nakuru":  "Rift Valley",
    "Kisumu":  "Nyanza"
}
channels = ["Direct", "Broker", "Online", "Bancassurance"]

source_agents = pd.read_sql("SELECT * FROM agents", engine)

try:
    existing_agents = pd.read_sql("SELECT agent_id FROM dim_agent", engine)
except Exception:
    existing_agents = pd.DataFrame()
if not existing_agents.empty and "agent_id" in existing_agents.columns:
    source_agents = source_agents[~source_agents["agent_id"].isin(existing_agents["agent_id"])]

if not source_agents.empty:
    dim_agent = pd.DataFrame({
        "agent_id":   source_agents["agent_id"],
        "agent_name": source_agents["full_name"],
        "agent_code": source_agents["agent_id"].astype(str).apply(lambda x: x[:8].upper()),
        "region":     source_agents["county"].map(regions_map).fillna("Other"),
        "channel":    [random.choice(channels) for _ in range(len(source_agents))],
        "hire_date":  pd.to_datetime(source_agents["hire_date"]).dt.date,
        "is_active":  source_agents["status"] == "Active"
    })
    dim_agent.to_sql("dim_agent", engine, if_exists="append", index=False, method="multi")
    print(f"  dim_agent loaded: {len(dim_agent)} rows")
else:
    print("  dim_agent already populated")


# ============================================================
# VERIFICATION
# ============================================================
print("\n" + "="*55)
print("STAR SCHEMA DIMENSIONS — FINAL VERIFICATION")
print("="*55)

tables = [
    "dim_date", "dim_customer", "dim_policy_type",
    "dim_county", "dim_agent", "dim_claim_status"
]

with engine.begin() as conn:
    for table in tables:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()
        print(f"  DIM  {table:<25} {count:>6} rows")

print("="*55)
print("\nDimensions fully loaded and verified.")
print("Ready for fact table ETL.")
