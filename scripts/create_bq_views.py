"""
BigQuery-native analytics views.
Creates reusable SQL views on top of the BigQuery fact/dim tables,
mirroring the analyses from 11_analytics.py.
"""

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.gcp import get_bq, PROJECT_ID, BQ_DATASET
from config.logging_setup import setup_logging

log = setup_logging(__name__)
bq = get_bq()

VIEWS = {
    "v_premium_by_policy_type": """
        CREATE OR REPLACE VIEW `{project}.{dataset}.v_premium_by_policy_type` AS
        SELECT
            pt.policy_type,
            COUNT(*)                AS policies,
            ROUND(AVG(fs.premium_amount), 2) AS avg_premium,
            ROUND(SUM(fs.premium_amount), 2) AS total_premium
        FROM `{project}.{dataset}.fact_sales` fs
        JOIN `{project}.{dataset}.dim_policy_type` pt USING (policy_type_id)
        GROUP BY pt.policy_type
        ORDER BY total_premium DESC
    """,
    "v_claims_summary": """
        CREATE OR REPLACE VIEW `{project}.{dataset}.v_claims_summary` AS
        SELECT
            cs.status_name,
            COUNT(*)                AS claims,
            ROUND(SUM(fc.claim_amount), 2)    AS total_claimed,
            ROUND(SUM(fc.approved_amount), 2) AS total_approved,
            ROUND(AVG(fc.approved_amount / NULLIF(fc.claim_amount, 0)) * 100, 1) AS approval_pct
        FROM `{project}.{dataset}.fact_claims` fc
        JOIN `{project}.{dataset}.dim_claim_status` cs ON fc.status_id = cs.status_id
        GROUP BY cs.status_name
        ORDER BY claims DESC
    """,
    "v_payment_methods": """
        CREATE OR REPLACE VIEW `{project}.{dataset}.v_payment_methods` AS
        SELECT
            payment_method,
            COUNT(*)                AS transactions,
            ROUND(SUM(payment_amount), 2)      AS total_collected,
            ROUND(AVG(payment_amount), 2)      AS avg_payment
        FROM `{project}.{dataset}.fact_payments`
        GROUP BY payment_method
        ORDER BY total_collected DESC
    """,
    "v_late_payments": """
        CREATE OR REPLACE VIEW `{project}.{dataset}.v_late_payments` AS
        SELECT
            pt.policy_type,
            COUNT(*) FILTER (WHERE fp.is_late = TRUE)  AS late_payments,
            COUNT(*) FILTER (WHERE fp.is_late = FALSE) AS on_time_payments,
            ROUND(AVG(fp.days_late), 1) AS avg_days_late
        FROM `{project}.{dataset}.fact_payments` fp
        JOIN `{project}.{dataset}.dim_policy_type` pt ON fp.policy_type_id = pt.policy_type_id
        GROUP BY pt.policy_type
        ORDER BY late_payments DESC
    """,
    "v_monthly_trends": """
        CREATE OR REPLACE VIEW `{project}.{dataset}.v_monthly_trends` AS
        SELECT
            DATE_TRUNC(date_key, MONTH) AS month,
            COUNT(DISTINCT sale_id)     AS policies_sold,
            ROUND(SUM(premium_amount), 2)     AS premium_volume
        FROM `{project}.{dataset}.fact_sales`
        GROUP BY month
        ORDER BY month
    """,
    "v_summary_stats": """
        CREATE OR REPLACE VIEW `{project}.{dataset}.v_summary_stats` AS
        SELECT 'Total Customers'   AS metric, CAST(COUNT(*) AS STRING) AS value FROM `{project}.{dataset}.dim_customer`
        UNION ALL
        SELECT 'Total Policies'   , CAST(COUNT(*) AS STRING) FROM `{project}.{dataset}.fact_sales`
        UNION ALL
        SELECT 'Total Claims'    , CAST(COUNT(*) AS STRING) FROM `{project}.{dataset}.fact_claims`
        UNION ALL
        SELECT 'Total Payments'  , CAST(COUNT(*) AS STRING) FROM `{project}.{dataset}.fact_payments`
        UNION ALL
        SELECT 'Total Premium'   , CAST(ROUND(SUM(premium_amount),0) AS STRING) FROM `{project}.{dataset}.fact_sales`
        UNION ALL
        SELECT 'Total Claimed'   , CAST(ROUND(SUM(claim_amount),0) AS STRING) FROM `{project}.{dataset}.fact_claims`
        UNION ALL
        SELECT 'Total Collected' , CAST(ROUND(SUM(payment_amount),0) AS STRING) FROM `{project}.{dataset}.fact_payments`
    """,
}

if __name__ == "__main__":
    log.info("Creating BigQuery analytics views...")
    for name, ddl in VIEWS.items():
        sql = ddl.format(project=PROJECT_ID, dataset=BQ_DATASET)
        try:
            bq.query(sql).result()
            log.info(f"  {name}")
        except Exception as e:
            log.error(f"  {name}: {e}")
    log.info("BigQuery views created.")
