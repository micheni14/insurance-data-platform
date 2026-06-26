"""
Phase 8 - Kafka Producer
Simulates real-time insurance events (new policies, claims, payments)
and streams them to the 'insurance-events' topic.
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# -- Config ------------------------------------------------------------------
TOPIC = "insurance-events"
BROKER = "kafka:9092"
DELAY_SECONDS = 1  # 1 event per second

POLICY_TYPES   = ["Auto", "Home", "Life", "Health", "Business"]
CLAIM_STATUSES = ["Pending", "Approved", "Rejected", "Under Review"]
PAYMENT_METHODS = ["M-Pesa", "Bank Transfer", "Credit Card", "Cash"]
COUNTIES = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret",
            "Thika", "Malindi", "Kitale", "Garissa", "Nyeri"]

EVENT_TYPES = ["new_policy", "new_claim", "new_payment"]

# -- Producer ----------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def generate_policy_event():
    return {
        "event_type": "new_policy",
        "policy_id": str(uuid.uuid4()),
        "customer_name": fake.name(),
        "policy_type": random.choice(POLICY_TYPES),
        "premium_amount": round(random.uniform(5000, 80000), 2),
        "county": random.choice(COUNTIES),
        "start_date": fake.date_between(start_date="-1y", end_date="today").isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

def generate_claim_event():
    return {
        "event_type": "new_claim",
        "claim_id": str(uuid.uuid4()),
        "policy_id": str(uuid.uuid4()),
        "claim_amount": round(random.uniform(10000, 500000), 2),
        "status": random.choice(CLAIM_STATUSES),
        "incident_date": fake.date_between(start_date="-6m", end_date="today").isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

def generate_payment_event():
    return {
        "event_type": "new_payment",
        "payment_id": str(uuid.uuid4()),
        "policy_id": str(uuid.uuid4()),
        "amount_paid": round(random.uniform(1000, 50000), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_date": datetime.now(timezone.utc).date().isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

GENERATORS = {
    "new_policy":  generate_policy_event,
    "new_claim":   generate_claim_event,
    "new_payment": generate_payment_event,
}

# -- Stream ------------------------------------------------------------------
print("=" * 55)
print("INSURANCE KAFKA PRODUCER STARTED")
print(f"   Topic  : {TOPIC}")
print(f"   Broker : {BROKER}")
print(f"   Rate   : 1 event / {DELAY_SECONDS}s")
print("=" * 55)

count = 0
try:
    while True:
        event_type = random.choice(EVENT_TYPES)
        event = GENERATORS[event_type]()

        producer.send(TOPIC, key=event_type, value=event)
        count += 1

        print(f"[{count:04d}] {event_type:<14} | "
              f"{event.get('county', event.get('status', event.get('payment_method', '')))} | "
              f"KES {event.get('premium_amount', event.get('claim_amount', event.get('amount_paid', 0))):,.2f}")

        time.sleep(DELAY_SECONDS)

except KeyboardInterrupt:
    print(f"\nProducer stopped. Total events sent: {count}")
    producer.flush()
    producer.close()
