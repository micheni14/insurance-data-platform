import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db import engine
from config.dedupe import deduplicate
from config.gcp import get_bq, BQ_DATASET, PROJECT_ID, dump_df_to_gcs
from config.logging_setup import setup_logging
import pandas as pd
from datetime import datetime
from sqlalchemy import text

log = setup_logging(__name__)

# 1. EXTRACT
def extract():
    log.info("EXTRACT — pulling source tables...")
    claims       = pd.read_sql("SELECT * FROM claims", engine)
    customers    = pd.read_sql("SELECT customer_id, county FROM dim_customer", engine)
    policies     = pd.read_sql("SELECT policy_id, policy_type FROM policies", engine)
    counties     = pd.read_sql("SELECT county_id, county_name FROM dim_county", engine)
    statuses     = pd.read_sql("SELECT status_id, status_name FROM dim_claim_status", engine)
    policy_types = pd.read_sql("SELECT policy_type_id, policy_type FROM dim_policy_type", engine)
    log.info(f"  claims       : {len(claims):,} rows")
    log.info(f"  customers    : {len(customers):,} rows")
    log.info(f"  policies     : {len(policies):,} rows")
    log.info(f"  counties     : {len(counties):,} rows")
    log.info(f"  statuses     : {len(statuses):,} rows")
    log.info(f"  policy_types : {len(policy_types):,} rows")
    return claims, customers, policies, counties, statuses, policy_types

# 2. VALIDATE
def validate(claims):
    log.info("VALIDATE — checking source data quality...")
    issues = []
    issues.append(("null claim_id",        claims["claim_id"].isna().sum()))
    issues.append(("null customer_id",     claims["customer_id"].isna().sum()))
    issues.append(("null policy_id",       claims["policy_id"].isna().sum()))
    issues.append(("negative claim_amount",(claims["claim_amount"] < 0).sum()))
    issues.append(("null claim_amount",    claims["claim_amount"].isna().sum()))
    claims["claim_date"]    = pd.to_datetime(claims["claim_date"])
    claims["incident_date"] = pd.to_datetime(claims["incident_date"])
    bad_dates = (claims["claim_date"] < claims["incident_date"]).sum()
    issues.append(("claim_date before incident_date", bad_dates))
    has_issues = False
    for name, count in issues:
        if count > 0:
            log.warning(f"  {name}: {count}")
            has_issues = True
    if not has_issues:
        log.info("  All source validations passed")
    return issues

# 3. TRANSFORM
def transform(claims, customers, policies, counties, statuses, policy_types):
    log.info("TRANSFORM — building fact_claims structure...")
    df = claims.copy()
    df["claim_date"]    = pd.to_datetime(df["claim_date"]).dt.date
    df["incident_date"] = pd.to_datetime(df["incident_date"]).dt.date
    df = df.merge(customers, on="customer_id", how="left")
    df = df.merge(counties, left_on="county", right_on="county_name", how="left")
    df = df.merge(policies, on="policy_id", how="left")
    df = df.merge(policy_types, on="policy_type", how="left")
    df = df.merge(statuses, left_on="claim_status", right_on="status_name", how="left")
    unmatched_status = df["status_id"].isna().sum()
    unmatched_policy = df["policy_type_id"].isna().sum()
    unmatched_county = df["county_id"].isna().sum()
    if unmatched_status  > 0: log.warning(f"  Unmatched statuses     : {unmatched_status}")
    if unmatched_policy  > 0: log.warning(f"  Unmatched policy types : {unmatched_policy}")
    if unmatched_county  > 0: log.warning(f"  Unmatched counties     : {unmatched_county}")
    fact = pd.DataFrame({
        "claim_id":          df["claim_id"].astype(str),
        "policy_id":         df["policy_id"].astype(str),
        "customer_id":       df["customer_id"].astype(str),
        "date_key":          df["claim_date"],
        "incident_date_key": df["incident_date"],
        "status_id":         df["status_id"],
        "county_id":         df["county_id"],
        "policy_type_id":    df["policy_type_id"],
        "claim_type":        df["claim_type"],
        "claim_amount":      pd.to_numeric(df["claim_amount"],   errors="coerce"),
        "approved_amount":   pd.to_numeric(df["approved_amount"],errors="coerce").fillna(0),
        "claim_status":      df["claim_status"],
        "incident_date":     df["incident_date"],
        "claim_date":        df["claim_date"],
        "created_at":        datetime.now()
    })
    log.info(f"  Transformed rows : {len(fact):,}")
    log.info(f"  Null check       : {fact.isnull().sum().to_dict()}")
    return fact

# 4. DEDUPLICATE
def deduplicate_fact(df):
    return deduplicate(df, engine, "fact_claims", "claim_id")

# 5. LOAD
def load(df):
    if df.empty:
        log.info("No new rows to load — skipping")
        return
    log.info(f"LOAD — inserting {len(df):,} rows into fact_claims...")
    df.to_sql("fact_claims", engine, if_exists="append", index=False, chunksize=100, method="multi")
    log.info("  Load complete")

# 5b. LOAD TO BIGQUERY
def load_bq(df):
    if df.empty:
        log.info("No rows to write to BigQuery — skipping")
        return
    bq = get_bq()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.fact_claims"
    log.info(f"LOAD_BQ: writing {len(df):,} rows to {table_id} ...")
    job = bq.load_table_from_dataframe(df, table_id, job_config={
        "write_disposition": "WRITE_APPEND",
        "autodetect": False,
    })
    job.result()
    log.info(f"  BigQuery load complete: {len(df):,} rows")

# 6. VERIFY
def verify():
    log.info("VERIFY — post-load reconciliation...")
    with engine.connect() as conn:
        total_source = conn.execute(text("SELECT COUNT(*) FROM claims")).scalar()
        total_fact   = conn.execute(text("SELECT COUNT(*) FROM fact_claims")).scalar()
        missing = conn.execute(text("SELECT COUNT(*) FROM claims c LEFT JOIN fact_claims f ON c.claim_id::text = f.claim_id::text WHERE f.claim_id IS NULL")).scalar()
        null_amounts = conn.execute(text("SELECT COUNT(*) FROM fact_claims WHERE claim_amount IS NULL")).scalar()
    log.info(f"  Source claims  : {total_source:,}")
    log.info(f"  Fact claims    : {total_fact:,}")
    log.info(f"  Missing rows   : {missing}")
    log.info(f"  Null amounts   : {null_amounts}")
    summary = pd.read_sql("SELECT cs.status_name, COUNT(*) AS claims, SUM(fc.claim_amount) AS total_claimed, SUM(fc.approved_amount) AS total_approved FROM fact_claims fc JOIN dim_claim_status cs ON fc.status_id = cs.status_id GROUP BY cs.status_name ORDER BY claims DESC", engine)
    log.info(f"\n{summary.to_string(index=False)}")
    if missing == 0 and null_amounts == 0:
        log.info("  PERFECT RECONCILIATION — all checks passed")
    else:
        log.warning("  DATA QUALITY ISSUES DETECTED — investigate above")

if __name__ == "__main__":
    start = datetime.now()
    log.info("=" * 55)
    log.info("FACT_CLAIMS ETL PIPELINE STARTED")
    log.info("=" * 55)
    try:
        claims, customers, policies, counties, statuses, policy_types = extract()
        validate(claims)
        fact = transform(claims, customers, policies, counties, statuses, policy_types)
        fact = deduplicate_fact(fact)
        dump_df_to_gcs(fact, "fact_claims")
        load(fact)
        load_bq(fact)
        verify()
        duration = (datetime.now() - start).seconds
        log.info("=" * 55)
        log.info(f"PIPELINE COMPLETE in {duration}s")
        log.info("=" * 55)
    except Exception as e:
        log.error(f"PIPELINE FAILED: {e}")
        raise
