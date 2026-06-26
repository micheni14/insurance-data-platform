import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db import engine
from config.dedupe import deduplicate
from config.gcp import get_bq, BQ_DATASET, PROJECT_ID, dump_df_to_gcs
from config.logging_setup import setup_logging
import uuid
import random
import pandas as pd
from datetime import datetime
from sqlalchemy import text

log = setup_logging(__name__)

# 1. EXTRACT
def extract():
    log.info("EXTRACT: pulling source tables...")
    policies     = pd.read_sql("SELECT * FROM policies",        engine)
    customers    = pd.read_sql("SELECT customer_id, county FROM dim_customer", engine)
    policy_types = pd.read_sql("SELECT policy_type_id, policy_type FROM dim_policy_type", engine)
    counties     = pd.read_sql("SELECT county_id, county_name FROM dim_county", engine)
    agents       = pd.read_sql("SELECT agent_id FROM dim_agent WHERE is_active = TRUE", engine)
    log.info(f"  policies     : {len(policies):,} rows")
    log.info(f"  dim_customer : {len(customers):,} rows")
    log.info(f"  dim_policy_type: {len(policy_types)} rows")
    log.info(f"  dim_county   : {len(counties)} rows")
    log.info(f"  dim_agent    : {len(agents)} rows")
    return policies, customers, policy_types, counties, agents

# 2. VALIDATE
def validate_source(policies, customers):
    log.info("VALIDATE: checking source data quality...")
    issues = []
    for col in ["policy_id", "customer_id", "premium", "start_date"]:
        null_count = policies[col].isna().sum()
        if null_count > 0:
            issues.append(f"NULL values in policies.{col}: {null_count}")
    out_of_range = policies[(policies["premium"] < 0) | (policies["premium"] > 1_000_000)]
    if len(out_of_range) > 0:
        issues.append(f"Premiums out of range: {len(out_of_range)} rows")
    bad_dates = policies[policies["end_date"] < policies["start_date"]]
    if len(bad_dates) > 0:
        issues.append(f"end_date before start_date: {len(bad_dates)} rows")
    unmatched = ~policies["customer_id"].astype(str).isin(customers["customer_id"].astype(str))
    if unmatched.sum() > 0:
        issues.append(f"Unmatched customer_ids: {unmatched.sum()} rows")
    if issues:
        for issue in issues:
            log.warning(f"  {issue}")
    else:
        log.info("  All source validations passed")
    return issues

# 3. TRANSFORM
def transform(policies, customers, policy_types, counties, agents):
    log.info("TRANSFORM: building fact_sales structure...")
    df = policies.copy()
    df = df.merge(customers[["customer_id", "county"]], on="customer_id", how="left")
    df = df.merge(policy_types[["policy_type_id", "policy_type"]], on="policy_type", how="left")
    df = df.merge(counties[["county_id", "county_name"]], left_on="county", right_on="county_name", how="left")
    agent_ids = agents["agent_id"].tolist()
    df["sale_id"]           = [str(uuid.uuid4()) for _ in range(len(df))]
    df["agent_id"]          = [random.choice(agent_ids) for _ in range(len(df))]
    df["date_key"]          = df["start_date"]
    df["commission_rate"]   = [round(random.uniform(0.05, 0.15), 4) for _ in range(len(df))]
    fact_sales = df[["sale_id", "policy_id", "customer_id", "agent_id", "date_key",
                     "policy_type_id", "county_id", "premium", "commission_rate",
                     "policy_type", "start_date", "end_date", "status"]].copy()
    fact_sales.rename(columns={"premium": "premium_amount", "status": "policy_status"}, inplace=True)
    fact_sales["sale_id"]           = fact_sales["sale_id"].astype(str)
    fact_sales["policy_id"]         = fact_sales["policy_id"].astype(str)
    fact_sales["customer_id"]       = fact_sales["customer_id"].astype(str)
    fact_sales["agent_id"]          = fact_sales["agent_id"].astype(str)
    fact_sales["premium_amount"]    = pd.to_numeric(fact_sales["premium_amount"], errors="coerce")
    fact_sales["date_key"]          = pd.to_datetime(fact_sales["date_key"]).dt.date
    log.info(f"  Transformed : {len(fact_sales):,} rows")
    log.info(f"  Columns     : {list(fact_sales.columns)}")
    log.info(f"  Null check  : {fact_sales.isnull().sum().to_dict()}")
    return fact_sales

# 4. DEDUPLICATE
def deduplicate_fact(fact_sales):
    return deduplicate(fact_sales, engine, "fact_sales", "policy_id")

# 5. LOAD
def load(fact_sales):
    if len(fact_sales) == 0:
        log.info("No new rows to insert — skipping load")
        return
    log.info(f"LOAD: inserting {len(fact_sales):,} rows into fact_sales...")
    fact_sales.to_sql("fact_sales", engine, if_exists="append", index=False, chunksize=100, method="multi")
    log.info("  Load complete")

# 5b. LOAD TO BIGQUERY
def load_bq(fact_sales):
    if fact_sales.empty:
        log.info("No rows to write to BigQuery — skipping")
        return
    bq = get_bq()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.fact_sales"
    log.info(f"LOAD_BQ: writing {len(fact_sales):,} rows to {table_id} ...")
    job = bq.load_table_from_dataframe(fact_sales, table_id, job_config={
        "write_disposition": "WRITE_APPEND",
        "autodetect": False,
        "schema_update_options": ["ALLOW_FIELD_ADDITION"],
    })
    job.result()
    log.info(f"  BigQuery load complete: {len(fact_sales):,} rows")

# 6. VERIFY
def verify():
    log.info("VERIFY: post-load checks...")
    with engine.connect() as conn:
        total    = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
        nulls    = conn.execute(text("SELECT COUNT(*) FROM fact_sales WHERE premium_amount IS NULL")).scalar()
        orphans  = conn.execute(text("SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_customer dc ON fs.customer_id = dc.customer_id WHERE dc.customer_id IS NULL")).scalar()
        by_type  = pd.read_sql("SELECT policy_type, COUNT(*) as cnt, ROUND(SUM(premium_amount),2) as total_premium FROM fact_sales GROUP BY policy_type ORDER BY total_premium DESC", engine)
    log.info(f"  Total rows   : {total:,}")
    log.info(f"  Null premiums: {nulls}")
    log.info(f"  Orphaned rows: {orphans}")
    log.info(f"\n{by_type.to_string(index=False)}")
    if nulls == 0 and orphans == 0:
        log.info("  All post-load checks passed")
    else:
        log.warning("  Post-load issues found — investigate above")

if __name__ == "__main__":
    start_time = datetime.now()
    log.info("=" * 55)
    log.info("fact_sales ETL PIPELINE STARTED")
    log.info("=" * 55)
    try:
        policies, customers, policy_types, counties, agents = extract()
        issues       = validate_source(policies, customers)
        fact_sales   = transform(policies, customers, policy_types, counties, agents)
        fact_sales   = deduplicate_fact(fact_sales)
        dump_df_to_gcs(fact_sales, "fact_sales")
        load(fact_sales)
        load_bq(fact_sales)
        verify()
        duration = (datetime.now() - start_time).seconds
        log.info("=" * 55)
        log.info(f"PIPELINE COMPLETE in {duration}s")
        log.info("=" * 55)
    except Exception as e:
        log.error(f"PIPELINE FAILED: {e}")
        raise
