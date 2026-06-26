"""
Phase 8 — Kafka Consumer
Reads insurance events from the 'insurance-events' topic
and inserts them into PostgreSQL in real time.
"""

import json
import os
import psycopg2
from datetime import datetime, timezone, timezone
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
TOPIC  = "insurance-events"
BROKER = "kafka:9092"
GROUP  = "insurance-consumer-group"

# ── DB Connection ─────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", 5432),
    dbname=os.getenv("DB_NAME", "insurance_db"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", ""),
)
conn.autocommit = True
cur = conn.cursor()

# ── Create staging table (runs once) ─────────────────────────────────────────
cur.execute("""
    CREATE TABLE IF NOT EXISTS kafka_events_log (
        id              SERIAL PRIMARY KEY,
        event_type      VARCHAR(50),
        event_id        VARCHAR(36),
        policy_id       VARCHAR(36),
        amount          NUMERIC(15,2),
        extra_info      VARCHAR(100),
        event_timestamp TIMESTAMP,
        ingested_at     TIMESTAMP DEFAULT NOW()
    );
""")
print("✅ kafka_events_log table ready")

# ── Consumer ──────────────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id=GROUP,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

def insert_event(event):
    etype = event.get("event_type")

    if etype == "new_policy":
        cur.execute("""
            INSERT INTO kafka_events_log
                (event_type, event_id, policy_id, amount, extra_info, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            etype,
            event.get("policy_id"),
            event.get("policy_id"),
            event.get("premium_amount"),
            event.get("county"),
            event.get("timestamp"),
        ))

    elif etype == "new_claim":
        cur.execute("""
            INSERT INTO kafka_events_log
                (event_type, event_id, policy_id, amount, extra_info, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            etype,
            event.get("claim_id"),
            event.get("policy_id"),
            event.get("claim_amount"),
            event.get("status"),
            event.get("timestamp"),
        ))

    elif etype == "new_payment":
        cur.execute("""
            INSERT INTO kafka_events_log
                (event_type, event_id, policy_id, amount, extra_info, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            etype,
            event.get("payment_id"),
            event.get("policy_id"),
            event.get("amount_paid"),
            event.get("payment_method"),
            event.get("timestamp"),
        ))

# ── Listen ────────────────────────────────────────────────────────────────────
print("=" * 55)
print("👂 INSURANCE KAFKA CONSUMER LISTENING")
print(f"   Topic : {TOPIC}")
print(f"   Group : {GROUP}")
print("=" * 55)

count = 0
try:
    for message in consumer:
        event = message.value
        insert_event(event)
        count += 1

        etype = event.get("event_type", "unknown")
        icon = {"new_policy": "📋", "new_claim": "🚨", "new_payment": "💳"}.get(etype, "📨")
        print(f"[{count:04d}] {icon}  {etype:<14} | "
              f"offset={message.offset} | "
              f"KES {event.get('premium_amount', event.get('claim_amount', event.get('amount_paid', 0))):,.2f} "
              f"→ 💾 DB")

except KeyboardInterrupt:
    print(f"\n✅ Consumer stopped. Total events ingested: {count}")
    cur.close()
    conn.close()
    consumer.close()