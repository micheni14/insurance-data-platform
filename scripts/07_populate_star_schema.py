import os
import uuid
import random
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
from faker import Faker

# -----------------------------
# 0. SETUP
# -----------------------------
load_dotenv()
fake = Faker()

print("Starting star schema ingestion pipeline...")

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD'))}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True
)

with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
print("Connected to PostgreSQL\n")


# ============================================================
# DIMENSION 1: dim_date
# Populate every date from 2019-01-01 to 2030-12-31
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
        "day_of_week":    current.isoweekday(),        # 1=Mon, 7=Sun
        "day_name":       current.strftime("%A"),
        "is_weekend":     current.isoweekday() >= 6,
        "is_month_start": current.day == 1,
        "is_month_end":   (current + timedelta(days=1)).month != current.month,
        "week_of_year":   current.isocalendar()[1]
    })
    current += timedelta(days=1)

df_date = pd.DataFrame(dates)

# Upsert — skip existing dates
try:
    existing = pd.read_sql("SELECT date_key FROM dim_date", engine)
    df_date  = df_date[~df_date["date_key"].isin(existing["date_key"])]
except Exception as e:
    print(f"WARNING: Could not read existing dim_date records: {e}")

if len(df_date) > 0:
    df_date.to_sql("dim_date", engine, if_exists="append", index=False, chunksize=100, method=None)
    print(f"dim_date loaded: {len(df_date)} rows")
else:
    print("dim_date already populated")


# ============================================================
# DIMENSION 2: dim_customer
# Copy & enrich from existing customers table
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

# Upsert
try:
    existing = pd.read_sql("SELECT customer_id FROM dim_customer", engine)
    dim_customer = dim_customer[~dim_customer["customer_id"].astype(str).isin(existing["customer_id"].astype(str))]
except Exception as e:
    print(f"WARNING: Could not read existing dim_customer records: {e}")

if len(dim_customer) > 0:
    dim_customer.to_sql("dim_customer", engine, if_exists="append", index=False, chunksize=100, method=None)
    print(f"dim_customer loaded: {len(dim_customer)} rows")
else:
    print("dim_customer already populated")


# ============================================================
# DIMENSION 3: dim_agent
# Generate 20 fake agents
# ============================================================
print("\nPopulating dim_agent...")

try:
    existing_agents = pd.read_sql("SELECT COUNT(*) as cnt FROM dim_agent", engine)
    if existing_agents["cnt"].iloc[0] > 0:
        print("dim_agent already populated")
        agents_df = pd.read_sql("SELECT agent_id FROM dim_agent", engine)
    else:
        raise RuntimeError("dim_agent table is empty — seeding now")
except Exception as e:
    channels = ["Direct", "Broker", "Online", "Bancassurance"]
    regions  = ["Nairobi Metro", "Central", "Coast", "Rift Valley", "Nyanza"]

    agents = []
    for i in range(1, 21):
        agents.append({
            "agent_id":   str(uuid.uuid4()),
            "agent_name": fake.name(),
            "agent_code": f"AGT{str(i).zfill(4)}",
            "region":     random.choice(regions),
            "channel":    random.choice(channels),
            "hire_date":  fake.date_between(start_date="-8y", end_date="-1y"),
            "is_active":  random.choice([True, True, True, False])  # 75% active
        })

    agents_df = pd.DataFrame(agents)
    agents_df.to_sql("dim_agent", engine, if_exists="append", index=False, chunksize=100, method=None)
    print(f"dim_agent loaded: {len(agents_df)} rows")


# ============================================================
# FACT TABLE 1: fact_sales
# One row per policy — links to all dimensions
# ============================================================
print("\nPopulating fact_sales...")

policies     = pd.read_sql("SELECT * FROM policies", engine)
policy_types = pd.read_sql("SELECT policy_type_id, policy_type FROM dim_policy_type", engine)
counties     = pd.read_sql("SELECT county_id, county_name FROM dim_county", engine)
agents_list  = pd.read_sql("SELECT agent_id FROM dim_agent WHERE is_active = TRUE", engine)

policy_type_map = dict(zip(policy_types["policy_type"], policy_types["policy_type_id"]))
county_map      = dict(zip(counties["county_name"], counties["county_id"]))

# Join customers to get county
customers_county = pd.read_sql("SELECT customer_id, county FROM customers", engine)
policies = policies.merge(customers_county, on="customer_id", how="left")

fact_sales_rows = []
for _, row in policies.iterrows():
    sale_date = row["start_date"]

    # Make sure sale_date exists in dim_date
    if pd.isna(sale_date):
        continue

    fact_sales_rows.append({
        "sale_id":        str(uuid.uuid4()),
        "policy_id":      str(row["policy_id"]),
        "customer_id":    str(row["customer_id"]),
        "agent_id":       str(random.choice(agents_list["agent_id"].tolist())),
        "date_key":       sale_date,
        "policy_type_id": policy_type_map.get(row["policy_type"]),
        "county_id":      county_map.get(row.get("county")),
        "premium_amount": float(row["premium"]),
        "commission_rate": round(random.uniform(0.05, 0.15), 4),
        "policy_type":    row["policy_type"],
        "start_date":     row["start_date"],
        "end_date":       row["end_date"],
        "policy_status":  row["status"],
    })

df_sales = pd.DataFrame(fact_sales_rows)

# Upsert
try:
    existing = pd.read_sql("SELECT sale_id FROM fact_sales", engine)
    df_sales = df_sales[~df_sales["sale_id"].isin(existing["sale_id"])]
except Exception as e:
    print(f"WARNING: Could not read existing fact_sales records: {e}")

if len(df_sales) > 0:
    df_sales.to_sql("fact_sales", engine, if_exists="append", index=False, chunksize=100, method=None)
    print(f"fact_sales loaded: {len(df_sales)} rows")
else:
    print("fact_sales already populated")


# ============================================================
# FACT TABLE 2: fact_claims
# Generate 1-2 claims per policy (not all policies have claims)
# ============================================================
print("\nPopulating fact_claims...")

claim_types   = ["Medical", "Accident", "Death", "Disability", "Property Damage"]
claim_statuses = pd.read_sql("SELECT status_id, status_name FROM dim_claim_status", engine)
status_map    = dict(zip(claim_statuses["status_name"], claim_statuses["status_id"]))

claims = []
for _, row in policies.iterrows():
    # Only ~60% of policies have claims
    if random.random() > 0.6:
        continue

    num_claims = random.randint(1, 2)
    for _ in range(num_claims):
        # Claim happens between start_date and today
        start = pd.to_datetime(row["start_date"]).date()
        today = datetime.today().date()
        if start >= today:
            continue

        incident_date = fake.date_between(start_date=start, end_date=today)
        claim_date    = incident_date + timedelta(days=random.randint(1, 30))
        claim_amount  = round(random.uniform(5000, float(row["premium"]) * 3), 2)
        status_name   = random.choices(
            ["Pending", "Approved", "Rejected", "Paid", "Escalated"],
            weights=[20, 35, 15, 25, 5]
        )[0]
        approved = round(claim_amount * random.uniform(0.5, 1.0), 2) if status_name in ["Approved", "Paid"] else 0

        claims.append({
            "claim_id":          str(uuid.uuid4()),
            "policy_id":         str(row["policy_id"]),
            "customer_id":       str(row["customer_id"]),
            "date_key":          claim_date,
            "incident_date_key": incident_date,
            "status_id":         status_map.get(status_name),
            "county_id":         county_map.get(row.get("county")),
            "policy_type_id":    policy_type_map.get(row["policy_type"]),
            "claim_amount":      claim_amount,
            "approved_amount":   approved,
            "claim_type":        random.choice(claim_types),
            "claim_status":      status_name,
            "incident_date":     incident_date,
            "claim_date":        claim_date,
        })

df_claims = pd.DataFrame(claims)

# Upsert
try:
    existing = pd.read_sql("SELECT claim_id FROM fact_claims", engine)
    df_claims = df_claims[~df_claims["claim_id"].isin(existing["claim_id"])]
except Exception as e:
    print(f"WARNING: Could not read existing fact_claims records: {e}")

if len(df_claims) > 0:
    df_claims.to_sql("fact_claims", engine, if_exists="append", index=False, chunksize=100, method=None)
    print(f"fact_claims loaded: {len(df_claims)} rows")
else:
    print("fact_claims already populated")


# ============================================================
# VIEW: summary_stats
# Used by 11_analytics.py — SELECT * FROM summary_stats
# ============================================================
print("\nCreating summary_stats view...")

with engine.connect() as conn:
    conn.execute(text("""
        CREATE OR REPLACE VIEW summary_stats AS
        SELECT 'Total Customers'       AS metric, COUNT(*)::text AS value FROM customers
        UNION ALL
        SELECT 'Total Policies',        COUNT(*)::text            FROM policies
        UNION ALL
        SELECT 'Total Claims',          COUNT(*)::text            FROM claims
        UNION ALL
        SELECT 'Gross Premium (KES)',   ROUND(SUM(premium_amount), 2)::text
            FROM fact_sales
        UNION ALL
        SELECT 'Total Collected (KES)', ROUND(SUM(payment_amount), 2)::text
            FROM fact_payments
            WHERE payment_status = 'Completed'
    """))
    conn.commit()

print("summary_stats view created")


# ============================================================
# VERIFICATION — Full schema summary
# ============================================================
print("\n" + "="*55)
print("STAR SCHEMA — FINAL VERIFICATION")
print("="*55)

tables = [
    "dim_date", "dim_customer", "dim_policy_type",
    "dim_county", "dim_agent", "dim_claim_status",
    "fact_sales", "fact_claims"
]

with engine.connect() as conn:
    for table in tables:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        tag   = "DIM " if table.startswith("dim") else "FACT"
        print(f"  {tag}  {table:<25} {count:>6} rows")

print("="*55)
print("\nStar schema fully loaded and verified.")
print("Ready for analytics.")
