import json
import os
import signal
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders-topic")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "orders-dashboard-consumer")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "retail_streaming")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "orders")

if not MONGODB_URI:
    print("ERROR: MONGODB_URI is not set. Copy .env.example to .env and add your MongoDB Atlas URI.")
    sys.exit(1)

client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=8000)
try:
    client.admin.command("ping")
except PyMongoError as exc:
    print(f"ERROR: Could not connect to MongoDB Atlas: {exc}")
    sys.exit(1)

db = client[MONGODB_DB]
collection = db[MONGODB_COLLECTION]
collection.create_index([("event_id", ASCENDING)], unique=True)
collection.create_index([("event_timestamp", ASCENDING)])
collection.create_index([("order_id", ASCENDING)])

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=KAFKA_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    consumer_timeout_ms=1000,
)

running = True


def shutdown_handler(signum, frame):
    global running
    running = False
    print("\nStopping consumer...")


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

print(f"Connected to Kafka: {KAFKA_BROKER}")
print(f"Consuming topic: {KAFKA_TOPIC}")
print(f"Writing to MongoDB: {MONGODB_DB}.{MONGODB_COLLECTION}")

try:
    while running:
        records = consumer.poll(timeout_ms=1000, max_records=20)
        for _, messages in records.items():
            for message in messages:
                data = message.value
                data["ingested_at"] = datetime.now(timezone.utc).isoformat()
                data["kafka_partition"] = message.partition
                data["kafka_offset"] = message.offset

                # Upsert by event_id so restarting/replaying the Kafka stream does not
                # create duplicate dashboard rows.
                collection.update_one(
                    {"event_id": data["event_id"]},
                    {"$set": data},
                    upsert=True,
                )
                print(
                    f"Consumed {data.get('event_id')} | order={data.get('order_id')} "
                    f"| amount=₹{float(data.get('amount', 0)):,.2f}"
                )
finally:
    consumer.close()
    client.close()
    print("Consumer closed.")
