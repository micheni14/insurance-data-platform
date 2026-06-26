"""
Insurance Data Pipeline DAG
============================
Automates the full insurance data pipeline:
  1. Generate customers
  2. Load policies
  3. Load claims + agents (parallel)
  4. Load payments
  5. Populate star schema
  6. Run analytics


"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import subprocess
import sys
import os

# ── Default settings for all tasks ──────────────────────────────────────────
default_args = {
    "owner": "lewis",
    "depends_on_past": False,           # Don't wait for yesterday's run
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,                       # Retry once if a task fails
    "retry_delay": timedelta(minutes=5) # Wait 5 min before retrying
}

# ── Helper — runs a script and prints output ─────────────────────────────────
def run_script(script_name: str):
    """Runs a script from the /opt/airflow/scripts folder."""
    script_path = f"/opt/airflow/scripts/{script_name}"
    print(f"▶ Running: {script_path}")

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "DB_HOST":     os.getenv("INSURANCE_DB_HOST", "localhost"),
            "DB_PORT":     os.getenv("INSURANCE_DB_PORT", "5432"),
            "DB_NAME":     os.getenv("INSURANCE_DB_NAME", "insurance_dw"),
            "DB_USER":     os.getenv("INSURANCE_DB_USER", "postgres"),
            "DB_PASSWORD": os.getenv("INSURANCE_DB_PASSWORD", ""),
        }
    )

    # Print output so it appears in Airflow logs
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    # Non-zero exit = task failure
    if result.returncode != 0:
        raise Exception(f"❌ {script_name} failed with code {result.returncode}")

    print(f"✅ {script_name} completed successfully")


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="insurance_pipeline",
    description="Full insurance data pipeline — ETL to analytics",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 1 * * *",  # Every day at 1:00 AM
    catchup=False,                  # Don't backfill missed runs
    max_active_runs=1,              # Prevent overlapping runs
    tags=["insurance", "etl", "analytics"],
) as dag:

    # ── Task 1: Generate Customers ────────────────────────────────────────────
    generate_customers = PythonOperator(
        task_id="generate_customers",
        python_callable=run_script,
        op_kwargs={"script_name": "01_generate_customers.py"},
    )

    # ── Task 2: Load Policies ─────────────────────────────────────────────────
    load_policies = PythonOperator(
        task_id="load_policies",
        python_callable=run_script,
        op_kwargs={"script_name": "02_load_policies.py"},
    )

    # ── Task 3a: Load Claims ──────────────────────────────────────────────────
    load_claims = PythonOperator(
        task_id="load_claims",
        python_callable=run_script,
        op_kwargs={"script_name": "03_load_claims.py"},
    )

    # ── Task 3b: Load Agents (runs in parallel with claims) ───────────────────
    load_agents = PythonOperator(
        task_id="load_agents",
        python_callable=run_script,
        op_kwargs={"script_name": "04_load_agents.py"},
    )

    # ── Task 4: Load Sales ────────────────────────────────────────────────────
    load_sales = PythonOperator(
        task_id="load_sales",
        python_callable=run_script,
        op_kwargs={"script_name": "05_load_sales.py"},
    )

    # ── Task 5: Load Payments ─────────────────────────────────────────────────
    load_payments = PythonOperator(
        task_id="load_payments",
        python_callable=run_script,
        op_kwargs={"script_name": "06_load_payments.py"},
    )

    # ── Task 6: Populate Star Schema ──────────────────────────────────────────
    populate_star_schema = PythonOperator(
        task_id="populate_star_schema",
        python_callable=run_script,
        op_kwargs={"script_name": "07_populate_star_schema.py"},
    )

    # ── Task 7: Load Fact Sales ───────────────────────────────────────────────
    load_fact_sales = PythonOperator(
        task_id="load_fact_sales",
        python_callable=run_script,
        op_kwargs={"script_name": "08_load_fact_sales.py"},
    )

    # ── Task 8: Load Fact Claims ──────────────────────────────────────────────
    load_fact_claims = PythonOperator(
        task_id="load_fact_claims",
        python_callable=run_script,
        op_kwargs={"script_name": "09_load_fact_claims.py"},
    )

    # ── Task 9: ETL Fact Payments ─────────────────────────────────────────────
    etl_fact_payments = PythonOperator(
        task_id="etl_fact_payments",
        python_callable=run_script,
        op_kwargs={"script_name": "etl_fact_payments.py"},
    )

    # ── Task 10: Run Analytics & Generate Charts ──────────────────────────────
    run_analytics = PythonOperator(
        task_id="run_analytics",
        python_callable=run_script,
        op_kwargs={"script_name": "11_analytics.py"},
    )

    # ── Pipeline Order (Dependencies) ─────────────────────────────────────────
    #
    # generate_customers
    #         │
    #         ▼
    #   load_policies
    #         │
    #         ▼
    # load_claims ──── load_agents    ← these two run in PARALLEL
    #         │               │
    #         └──────┬────────┘
    #                ▼
    #           load_sales
    #                │
    #                ▼
    #          load_payments
    #                │
    #                ▼
    #     populate_star_schema
    #                │
    #         ┌──────┴──────┐
    #         ▼             ▼
    #   load_fact_sales  load_fact_claims   ← parallel
    #         │             │
    #         └──────┬──────┘
    #                ▼
    #       etl_fact_payments
    #                │
    #                ▼
    #          run_analytics

    generate_customers >> load_policies
    load_policies >> [load_claims, load_agents]
    [load_claims, load_agents] >> load_sales
    load_sales >> load_payments
    load_payments >> populate_star_schema
    populate_star_schema >> [load_fact_sales, load_fact_claims]
    [load_fact_sales, load_fact_claims] >> etl_fact_payments
    etl_fact_payments >> run_analytics