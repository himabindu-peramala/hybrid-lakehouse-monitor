import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'broker:29092'
TOPIC_NAME = 'user_clicks'
DELAY_SECONDS = 0.5  # 2 events per second

fake = Faker()

def create_kafka_producer():
    """Creates a Kafka Producer with JSON serialization."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

def generate_click_event():
    """Generates a random user click event."""
    # Data Dictionary / Schema
    event_types = ['view_item', 'add_to_cart', 'purchase', 'view_page']
    
    event = {
        "event_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999), # Simulate finite user base
        "session_id": fake.uuid4(),
        "event_type": random.choices(event_types, weights=[50, 20, 5, 25])[0], # Weighted random
        "product_id": random.randint(1, 100), # Links to our Batch "Product Catalog"
        "timestamp": time.time(),
        "url": fake.uri(),
        "device_type": random.choice(['mobile', 'desktop', 'tablet']),
        "city": fake.city()
    }
    return event

def main():
    producer = create_kafka_producer()
    if not producer:
        print("Exiting due to connection failure. (Is Docker running?)")
        return

    print(f"Starting to produce events to topic '{TOPIC_NAME}'...")
    try:
        while True:
            event = generate_click_event()
            producer.send(TOPIC_NAME, value=event)
            print(f"[Sent] {event['event_type']} by User {event['user_id']}")
            time.sleep(DELAY_SECONDS)
    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.close()

if __name__ == "__main__":
    main()
