"""Produce a mix of valid and malformed user signup events to Kafka."""

import json
import sys
import time

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "user-signups"

EVENTS = [
    {"user_id": "u1", "email": "alice@example.com", "name": "Alice", "age": 28, "signup_timestamp": "2024-01-01T00:00:00Z"},
    {"user_id": "u2", "email": "bob@example.com", "name": "Bob", "age": 34, "signup_timestamp": "2024-01-02T00:00:00Z"},
    {"user_id": "u3", "name": "Charlie", "age": 22, "signup_timestamp": "2024-01-03T00:00:00Z"},
    {"user_id": "u4", "email": "diana@example.com", "name": "Diana", "age": "twenty-five", "signup_timestamp": "2024-01-04T00:00:00Z"},
    {"user_id": "u5", "email": "eve@example.com", "name": "Eve", "age": 30, "signup_timestamp": "2024-01-05T00:00:00Z"},
]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for event in EVENTS:
    producer.send(TOPIC, event)
    label = "VALID   " if "email" in event and isinstance(event.get("age"), int) else "MALFORMED"
    print(f"[{label}] {event}")
    time.sleep(0.3)

producer.flush()
producer.close()
print(f"\nSent {len(EVENTS)} events to '{TOPIC}'")
