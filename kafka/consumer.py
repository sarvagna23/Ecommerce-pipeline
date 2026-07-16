import json
from kafka import KafkaConsumer

def consume_topic(topic: str, max_messages: int = 10):
    """
    Reads messages from a Kafka topic and prints them.
    max_messages — stops after reading this many messages.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",      # read from beginning
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,          # stop if no messages for 5 seconds
    )

    print(f"Reading from topic: '{topic}'")
    count = 0

    for message in consumer:
        print(f"  [{count+1}] offset={message.offset} | {message.value}")
        count += 1
        if count >= max_messages:
            break

    consumer.close()
    print(f"Read {count} messages from '{topic}'")


if __name__ == "__main__":
    print("=== Orders ===")
    consume_topic("ecommerce-orders", max_messages=5)

    print("\n=== Customers ===")
    consume_topic("ecommerce-customers", max_messages=5)

    print("\n=== Products ===")
    consume_topic("ecommerce-products", max_messages=5)