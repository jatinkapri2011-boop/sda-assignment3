import csv
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "orders-topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
)

print(f"Connected to Kafka broker at {KAFKA_BROKER}")
print(f"Sending messages to topic: {TOPIC}")

with open("sample_data.csv", mode="r", newline="") as file:
    reader = csv.DictReader(file)
    for row in reader:
        row["quantity"] = int(row["quantity"])
        row["amount"] = float(row["amount"])
        producer.send(TOPIC, key=row["order_id"], value=row)
        producer.flush()
        print("Sent:", json.dumps(row))
        time.sleep(1)

producer.close()
print("All messages sent successfully.")
