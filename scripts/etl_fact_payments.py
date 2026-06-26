import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
import random
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from config.db import engine
from config.dedupe import deduplicate
from config.gcp import get_bq, BQ_DATASET, PROJECT_ID, dump_df_to_gcs
from config.logging_setup import setup_logging

log = setup_logging(__name__)

# 0. CREATE TABLE
def create_table():
    log.info("Ensuring fact_payments table exists...")
    sql = """
    CREATE TABLE IF NOT EXISTS fact_payments (
        payment_id        VARCHAR(36)    PRIMARY KEY,
        policy_id         VARCHAR(36)    NOT NULL,
        customer_id       VARCHAR(36)    NOT NULL,
        date_key          DATE           NOT NULL REFERENCES dim_date(date_key),
        county_id         INTEGER        REFERENCES dim_county(county_id),
        policy_type_id    INTEGER        REFERENCES dim_policy_type(policy_type_id),
        payment_amount    NUMERIC(12,2)  NOT NULL,
        expected_amount   NUMERIC(12,2)  NOT NULL,
        variance          NUMERIC(12,2)  GENERATED ALWAYS AS (payment_amount - expected_amount) STORED,
        is_full_payment   BOOLEAN        GENERATED ALWAYS AS (payment_amount >= expected_amount) STORED,
        payment_method    VARCHAR(30),
        payment_status    VARCHAR(20),
        payment_frequency VARCHAR(20),
        is_late           BOOLEAN        DEFAULT FALSE,
        days_late         INTEGER        DEFAULT 0,
        payment_date      DATE,
        due_date          DATE,
        created_at        TIMESTAMP      DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    log.info("  fact_payments table ready!")

# 1. EXTRACT
def extract():
    log.info("EXTRACT — pulling source tables...")
    policies     = pd.read_sql("SELECT * FROM policies", engine)
    customers    = pd.read_sql("SELECT customer_id, county FROM dim_customer", engine)
    counties     = pd.read_sql("SELECT county_id, county_name FROM dim_county", engine)
    policy_types = pd.read_sql("SELECT policy_type_id, policy_type FROM dim_policy_type", engine)
    log.info(f"  policies     : {len(policies):,} rows")
    log.info(f"  customers    : {len(customers):,} rows")
    log.info(f"  counties     : {len(counties):,} rows")
    log.info(f"  policy_types : {len(policy_types):,} rows")
    return policies, customers, counties, policy_types

# 2. VALIDATE
def validate(policies):
    log.info("VALIDATE — checking source data quality...")
    issues = []
    issues.append(("null policy_id",    policies["policy_id"].isna().sum()))
    issues.append(("null customer_id",  policies["customer_id"].isna().sum()))
    issues.append(("null premium",      policies["premium"].isna().sum()))
    issues.append(("negative premium", (policies["premium"] < 0).sum()))
    issues.append(("null start_date",   policies["start_date"].isna().sum()))
    has_issues = False
    for name, count in issues:
        if count > 0:
            log.warning(f"  {name}: {count}")
            has_issues = True
    if not has_issues:
        log.info("  All source validations passed")
    return issues

# 3. TRANSFORM
def transform(policies, customers, counties, policy_types):
    log.info("TRANSFORM — generating payment records...")
    df = policies.merge(customers, on="customer_id", how="left")
    df = df.merge(counties, left_on="county", right_on="county_name", how="left")
    df = df.merge(policy_types, on="policy_type", how="left")
    payment_methods    = ["Mpesa", "Bank Transfer", "Cash", "Card"]
    payment_statuses   = ["Completed", "Completed", "Completed", "Pending", "Failed", "Reversed"]
    payment_frequencies = ["Monthly", "Quarterly", "Annual"]
    payments = []
    for _, row in df.iterrows():
        start_date = pd.to_datetime(row["start_date"]).date()
        end_date   = pd.to_datetime(row["end_date"]).date()
        today      = datetime.today().date()
        cutoff     = min(end_date, today)
        frequency  = random.choice(payment_frequencies)
        if frequency == "Monthly":
            interval_days  = 30
            installment    = round(float(row["premium"]) / 12, 2)
        elif frequency == "Quarterly":
            interval_days  = 90
            installment    = round(float(row["premium"]) / 4, 2)
        else:
            interval_days  = 365
            installment    = round(float(row["premium"]), 2)
        due_date = start_date
        while due_date <= cutoff:
            payment_date   = due_date + timedelta(days=random.randint(-3, 15))
            is_late        = payment_date > due_date
            days_late      = max(0, (payment_date - due_date).days)
            status         = random.choice(payment_statuses)
            payment_amount = installment
            if status == "Completed" and random.random() < 0.05:
                payment_amount = round(installment * random.uniform(0.7, 0.99), 2)
            elif status in ["Failed", "Reversed"]:
                payment_amount = 0.00
            payments.append({
                "payment_id":        str(uuid.uuid4()),
                "policy_id":         str(row["policy_id"]),
                "customer_id":       str(row["customer_id"]),
                "date_key":          payment_date,
                "county_id":         row.get("county_id"),
                "policy_type_id":    row.get("policy_type_id"),
                "payment_amount":    payment_amount,
                "expected_amount":   installment,
                "payment_method":    random.choice(payment_methods),
                "payment_status":    status,
                "payment_frequency": frequency,
                "is_late":           is_late,
                "days_late":         days_late,
                "payment_date":      payment_date,
                "due_date":          due_date,
            })
            due_date = due_date + timedelta(days=interval_days)
    fact = pd.DataFrame(payments)
    valid_dates = pd.read_sql("SELECT date_key FROM dim_date", engine)
    valid_dates["date_key"] = pd.to_datetime(valid_dates["date_key"]).dt.date
    fact["date_key"] = pd.to_datetime(fact["date_key"]).dt.date
    before = len(fact)
    fact = fact[fact["date_key"].isin(valid_dates["date_key"])]
    dropped = before - len(fact)
    if dropped > 0:
        log.warning(f"  Dropped {dropped} rows with dates outside dim_date range")
    log.info(f"  Transformed rows : {len(fact):,}")
    log.info(f"  Null check       : {fact.isnull().sum().to_dict()}")
    log.info(f"\n{fact['payment_method'].value_counts().to_string()}")
    log.info(f"\n{fact['payment_status'].value_counts().to_string()}")
    log.info(f"\n{fact['payment_frequency'].value_counts().to_string()}")
    return fact

# 4. DEDUPLICATE
def deduplicate_fact(df):
    return deduplicate(df, engine, "fact_payments", "payment_id")

# 5. LOAD
def load(df):
    if df.empty:
        log.info("No new rows to load — skipping")
        return
    log.info(f"LOAD — inserting {len(df):,} rows into fact_payments...")
    df.to_sql("fact_payments", engine, if_exists="append", index=False, chunksize=100, method="multi")
    log.info("  Load complete")

# 5b. LOAD TO BIGQUERY
def load_bq(df):
    if df.empty:
        log.info("No rows to write to BigQuery — skipping")
        return
    bq = get_bq()
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.fact_payments"
    log.info(f"LOAD_BQ: writing {len(df):,} rows to {table_id} ...")
    job = bq.load_table_from_dataframe(df, table_id, job_config={
        "write_disposition": "WRITE_APPEND",
        "autodetect": False,
    })
    job.result()
    log.info(f"  BigQuery load complete: {len(df):,} rows")

# 6. VERIFY
def verify():
    log.info("VERIFY — post-load checks...")
    with engine.connect() as conn:
        total        = conn.execute(text("SELECT COUNT(*) FROM fact_payments")).scalar()
        null_amounts = conn.execute(text("SELECT COUNT(*) FROM fact_payments WHERE payment_amount IS NULL")).scalar()
        late_payments = conn.execute(text("SELECT COUNT(*) FROM fact_payments WHERE is_late = TRUE")).scalar()
        failed = conn.execute(text("SELECT COUNT(*) FROM fact_payments WHERE payment_status IN ('Failed','Reversed')")).scalar()
        total_collected = conn.execute(text("SELECT ROUND(SUM(payment_amount),2) FROM fact_payments WHERE payment_status = 'Completed'")).scalar()
    by_method = pd.read_sql("SELECT payment_method, payment_status, COUNT(*) AS payments, ROUND(SUM(payment_amount), 2) AS total_amount FROM fact_payments GROUP BY payment_method, payment_status ORDER BY payments DESC LIMIT 10", engine)
    late_analysis = pd.read_sql("SELECT pt.policy_type, COUNT(*) FILTER (WHERE fp.is_late = TRUE) AS late_payments, COUNT(*) FILTER (WHERE fp.is_late = FALSE) AS on_time_payments, ROUND(AVG(fp.days_late), 1) AS avg_days_late FROM fact_payments fp JOIN dim_policy_type pt ON fp.policy_type_id = pt.policy_type_id GROUP BY pt.policy_type ORDER BY late_payments DESC", engine)
    log.info(f"  Total payments     : {total:,}")
    log.info(f"  Null amounts       : {null_amounts}")
    log.info(f"  Late payments      : {late_payments:,}")
    log.info(f"  Failed/Reversed    : {failed:,}")
    log.info(f"  Total collected    : KES {total_collected:,}")
    log.info(f"\nBy Method & Status:\n{by_method.to_string(index=False)}")
    log.info(f"\nLate Payment Analysis:\n{late_analysis.to_string(index=False)}")
    if null_amounts == 0:
        log.info("  All post-load checks passed")
    else:
        log.warning("  Issues found — investigate above")

if __name__ == "__main__":
    start = datetime.now()
    log.info("=" * 55)
    log.info("FACT_PAYMENTS ETL PIPELINE STARTED")
    log.info("=" * 55)
    try:
        create_table()
        policies, customers, counties, policy_types = extract()
        validate(policies)
        fact = transform(policies, customers, counties, policy_types)
        fact = deduplicate_fact(fact)
        dump_df_to_gcs(fact, "fact_payments")
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
