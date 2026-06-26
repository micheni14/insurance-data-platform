"""
Kafka → BigQuery streaming consumer.
Listens to 'insurance-events' topic and streams each event
directly into BigQuery for real-time analytics.
"""

import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from kafka import KafkaConsumer
from config.gcp import get_bq, PROJECT_ID, BQ_DATASET
from config.logging_setup import setup_logging

log = setup_logging(__name__)

TOPIC  = "insurance-events"
BROKER = "kafka:9092"
GROUP  = "insurance-bq-consumer-group"
TABLE  = f"{PROJECT_ID}.{BQ_DATASET}.kafka_events"

def ensure_table():
    bq = get_bq()
    sql = f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            event_type      STRING,
            event_id        STRING,
            policy_id       STRING,
            customer_name   STRING,
            amount          FLOAT64,
            extra_info      STRING,
            event_timestamp TIMESTAMP,
            ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        PARTITION BY DATE(ingested_at)
        OPTIONS(description="Real-time insurance events from Kafka")
    """
    try:
        bq.query(sql).result()
        log.info(f"Ensured table {TABLE}")
    except Exception as e:
        log.warning(f"Table create skipped (may already exist): {e}")

def row_from_event(event):
    etype = event.get("event_type", "unknown")
    base = {
        "event_type": etype,
        "policy_id": event.get("policy_id", ""),
        "event_timestamp": event.get("timestamp", datetime.now(timezone.utc).isoformat()),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "customer_name": event.get("customer_name", ""),
    }
    if etype == "new_policy":
        base["event_id"] = event.get("policy_id", "")
        base["amount"] = event.get("premium_amount", 0)
        base["extra_info"] = event.get("county", "")
    elif etype == "new_claim":
        base["event_id"] = event.get("claim_id", "")
        base["amount"] = event.get("claim_amount", 0)
        base["extra_info"] = event.get("status", "")
    elif etype == "new_payment":
        base["event_id"] = event.get("payment_id", "")
        base["amount"] = event.get("amount_paid", 0)
        base["extra_info"] = event.get("payment_method", "")
    else:
        base["event_id"] = str(event.get("event_id", ""))
        base["amount"] = 0
        base["extra_info"] = ""
    return base

def stream_to_bq(rows):
    if not rows:
        return
    bq = get_bq()
    errors = bq.insert_rows_json(TABLE, rows)
    if errors:
        log.error(f"BigQuery insert errors: {errors}")
    else:
        log.info(f"  Streamed {len(rows)} rows to {TABLE}")

if __name__ == "__main__":
    ensure_table()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    log.info("=" * 55)
    log.info(f"KAFKA → BIGQUERY STREAMING CONSUMER LISTENING")
    log.info(f"  Topic  : {TOPIC}")
    log.info(f"  Table  : {TABLE}")
    log.info("=" * 55)

    count = 0
    batch = []
    BATCH_SIZE = 10

    try:
        for message in consumer:
            event = message.value
            row = row_from_event(event)
            batch.append(row)
            count += 1

            etype = event.get("event_type", "unknown")
            amt = event.get("premium_amount", event.get("claim_amount", event.get("amount_paid", 0)))
            log.info(f"[{count:04d}] {etype:<14} | offset={message.offset} | KES {amt:,.2f} -> BQ")

            if len(batch) >= BATCH_SIZE:
                stream_to_bq(batch)
                batch = []

    except KeyboardInterrupt:
        if batch:
            stream_to_bq(batch)
        log.info(f"\nConsumer stopped. Total events streamed: {count}")
        consumer.close()
