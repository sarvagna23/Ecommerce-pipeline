import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# --- Connect to Kafka ---
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

ORDERS_FILE    = "/opt/airflow/data/generated/orders.csv"
CUSTOMERS_FILE = "/opt/airflow/data/generated/customers.csv"
PRODUCTS_FILE  = "/opt/airflow/data/generated/products.csv"


def stream_topic(filepath: str, topic: str, delay: float = 0.001):
    """
    Reads a CSV file and streams each row as a JSON message to a Kafka topic.
    delay=0.001 means 1000 messages per second — realistic order ingestion rate.
    """
    df = pd.read_csv(filepath)
    total = len(df)
    print(f"Streaming {total:,} records to topic '{topic}'...")

    for i, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic, value=message)

        if (i + 1) % 10_000 == 0:
            print(f"  sent {i+1:,} / {total:,} to '{topic}'")
            producer.flush()

        time.sleep(delay)

    producer.flush()
    print(f"Done streaming {total:,} records to '{topic}'")


if __name__ == "__main__":
    print("Starting Kafka producer...")
    print("Streaming customers...")
    stream_topic(CUSTOMERS_FILE, "ecommerce-customers", delay=0.0001)

    print("\nStreaming products...")
    stream_topic(PRODUCTS_FILE, "ecommerce-products", delay=0.0001)

    print("\nStreaming orders...")
    stream_topic(ORDERS_FILE, "ecommerce-orders", delay=0.001)

    print("\nAll data streamed to Kafka successfully.")