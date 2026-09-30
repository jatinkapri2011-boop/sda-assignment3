import csv
import json
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders-topic")
INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
)

rows = []
with open("sample_data.csv", mode="r", newline="") as file:
    rows = list(csv.DictReader(file))

if not rows:
    raise RuntimeError("sample_data.csv is empty")

print(f"Streaming continuously to {TOPIC} via {KAFKA_BROKER}")
print("Press Ctrl+C to stop.")

counter = 10001
base_time = datetime.now()

try:
    while True:
        template = rows[(counter - 10001) % len(rows)].copy()
        template["event_id"] = f"LIVE{counter}"
        template["order_id"] = f"L{counter}"
        template["customer_id"] = f"C{counter}"
        template["event_timestamp"] = (base_time + timedelta(seconds=counter - 10001)).isoformat(timespec="seconds")
        template["quantity"] = int(template["quantity"])
        template["amount"] = float(template["amount"])

        producer.send(TOPIC, key=template["order_id"], value=template)
        producer.flush()
        print(f"Sent {template['event_id']} | {template['product_id']} | ₹{template['amount']:,.2f}")

        counter += 1
        time.sleep(INTERVAL_SECONDS)
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()
