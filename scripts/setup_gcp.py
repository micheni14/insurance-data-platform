"""
GCP Infrastructure Setup  run once to create:
  1. Cloud Storage bucket (reports / data-lake)
  2. BigQuery dataset
  3. BigQuery fact tables matching PG schemas
"""
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.gcp import PROJECT_ID, GCS_BUCKET, BQ_DATASET, get_bq, get_gcs
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound, Conflict

bq = get_bq()
gcs = get_gcs()

#  1. Cloud Storage bucket 
print(f"\n Ensuring GCS bucket: gs://{GCS_BUCKET}/")
try:
    gcs.create_bucket(GCS_BUCKET, location="US")
    print("   Created")
except Conflict:
    print("   Already exists")

#  2. BigQuery dataset 
print(f"\n Ensuring BQ dataset: {PROJECT_ID}.{BQ_DATASET}")
ds_ref = bigquery.DatasetReference(PROJECT_ID, BQ_DATASET)
try:
    bq.create_dataset(bigquery.Dataset(ds_ref), exists_ok=True)
    print("   Ready")
except Conflict:
    print("   Already exists")

#  3. BigQuery fact tables 
FACT_TABLES = {
    "fact_sales": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_sales` (
            sale_id         STRING NOT NULL,
            policy_id       STRING,
            customer_id     STRING,
            agent_id        STRING,
            date_key        DATE,
            policy_type_id  INT64,
            county_id       INT64,
            premium_amount  FLOAT64,
            commission_rate FLOAT64,
            policy_type     STRING,
            start_date      DATE,
            end_date        DATE,
            policy_status   STRING
        ) PARTITION BY date_key
    """,
    "fact_claims": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_claims` (
            claim_id          STRING NOT NULL,
            policy_id         STRING,
            customer_id       STRING,
            date_key          DATE,
            incident_date_key DATE,
            status_id         INT64,
            county_id         INT64,
            policy_type_id    INT64,
            claim_type        STRING,
            claim_amount      FLOAT64,
            approved_amount   FLOAT64,
            claim_status      STRING,
            incident_date     DATE,
            claim_date        DATE,
            created_at        TIMESTAMP
        ) PARTITION BY date_key
    """,
    "fact_payments": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_payments` (
            payment_id        STRING NOT NULL,
            policy_id         STRING,
            customer_id       STRING,
            date_key          DATE,
            county_id         INT64,
            policy_type_id    INT64,
            payment_amount    FLOAT64,
            expected_amount   FLOAT64,
            payment_method    STRING,
            payment_status    STRING,
            payment_frequency STRING,
            is_late           BOOL,
            days_late         INT64,
            payment_date      DATE,
            due_date          DATE,
            created_at        TIMESTAMP
        ) PARTITION BY date_key
    """,
    "dim_date": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dim_date` (
            date_key      DATE NOT NULL,
            day           INT64,
            month         INT64,
            month_name    STRING,
            quarter       INT64,
            quarter_name  STRING,
            year          INT64,
            day_of_week   INT64,
            day_name      STRING,
            is_weekend    BOOL,
            is_month_start BOOL,
            is_month_end  BOOL,
            week_of_year  INT64
        )
    """,
    "dim_customer": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dim_customer` (
            customer_id     STRING NOT NULL,
            full_name       STRING,
            age             INT64,
            age_band        STRING,
            gender          STRING,
            county          STRING,
            region          STRING,
            signup_date     DATE,
            customer_tenure STRING,
            ingested_at     TIMESTAMP
        )
    """,
    "dim_agent": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dim_agent` (
            agent_id   STRING NOT NULL,
            agent_name STRING,
            agent_code STRING,
            region     STRING,
            channel    STRING,
            hire_date  DATE,
            is_active  BOOL
        )
    """,
    "dim_policy_type": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dim_policy_type` (
            policy_type_id INT64 NOT NULL,
            policy_type    STRING
        )
    """,
    "dim_county": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dim_county` (
            county_id   INT64 NOT NULL,
            county_name STRING
        )
    """,
    "dim_claim_status": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.dim_claim_status` (
            status_id   INT64 NOT NULL,
            status_name STRING
        )
    """,
    "kafka_events": """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.kafka_events` (
            event_type      STRING,
            event_id        STRING,
            policy_id       STRING,
            customer_name   STRING,
            amount          FLOAT64,
            extra_info      STRING,
            event_timestamp TIMESTAMP,
            ingested_at     TIMESTAMP
        ) PARTITION BY DATE(ingested_at)
    """,
}

print(f"\n  Creating BigQuery tables in {BQ_DATASET}...")
for name, ddl in FACT_TABLES.items():
    sql = ddl.format(project=PROJECT_ID, dataset=BQ_DATASET)
    try:
        bq.query(sql).result()
        print(f"   {name}")
    except Exception as e:
        print(f"   {name}: {e}")

print(f"\n{'='*55}")
print("GCP SETUP COMPLETE")
print(f"  Bucket : gs://{GCS_BUCKET}/")
print(f"  Dataset: {PROJECT_ID}.{BQ_DATASET}")
print(f"{'='*55}")

