import os
import json
import time
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

def main():
    # Inside docker compose network, Kafka is reachable at kafka:29092.
    # From host machine, it is reachable at localhost:9092.
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    # Auto-resolve Kafka address if running inside Docker container
    if os.path.exists("/.dockerenv") and "localhost" in bootstrap_servers:
        bootstrap_servers = "kafka:29092"
        
    topic_name = "users"

    print(f"Connecting to Kafka broker at: {bootstrap_servers}")
    
    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        retries=5
    )

    # Sample user records designed to demonstrate:
    # 1. Successful ingestion (records 1, 3, 5)
    # 2. Schema evolution (includes signup_ts, age)
    # 3. Deduplication (record 2 with a newer timestamp updates the older one; record 1 with an older timestamp is ignored)
    # 4. Invalidation/Delete demo (record 4 has a negative age, which will be filtered or deleted later)
    sample_records = [
        {"id": "1", "name": "Alice", "email": "alice@example.com", "signup_ts": "2023-01-01T12:00:00.000Z", "age": 25},
        {"id": "2", "name": "Bob", "email": "bob@example.com", "signup_ts": "2023-01-02T13:00:00.000Z", "age": 30},
        {"id": "3", "name": "Charlie", "email": "charlie@example.com", "signup_ts": "2023-01-03T14:00:00.000Z", "age": 35},
        # Newer duplicate of Bob (updates existing record)
        {"id": "2", "name": "Bob Updated", "email": "bob_new@example.com", "signup_ts": "2023-01-04T15:00:00.000Z", "age": 31},
        # Older duplicate of Alice (should NOT overwrite Alice's 2023 signup)
        {"id": "1", "name": "Alice Old", "email": "alice_old@example.com", "signup_ts": "2022-12-31T11:00:00.000Z", "age": 24},
        # Invalid record for manual data refinement / delete testing
        {"id": "4", "name": "Invalid User", "email": "invalid@example.com", "signup_ts": "2023-01-04T16:00:00.000Z", "age": -5},
        # Another valid user record
        {"id": "5", "name": "David", "email": "david@example.com", "signup_ts": "2023-01-05T17:00:00.000Z", "age": 40}
    ]

    print(f"Sending {len(sample_records)} messages to Kafka topic '{topic_name}'...")
    for record in sample_records:
        key = record["id"]
        print(f"Producing: Key={key} -> {record}")
        producer.send(topic_name, key=key, value=record)
        # Add a tiny delay to ensure timestamps are processed sequentially if needed
        time.sleep(0.1)

    producer.flush()
    print("All messages sent and flushed successfully.")

if __name__ == "__main__":
    main()
