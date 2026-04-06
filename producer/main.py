import os
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Configuration from Environment
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_USER_EVENTS = os.getenv("TOPIC_USER_EVENTS", "user-events")
TOPIC_CONTENT_METADATA = os.getenv("TOPIC_CONTENT_METADATA", "content-metadata")
TOPIC_FEATURE_STORE = os.getenv("TOPIC_FEATURE_STORE", "feature-store")

# Load submission IDs to ensure they are present in the simulation
with open('../submission.json', 'r') as f:
    submission_data = json.load(f)
    TEST_USER_ID = submission_data["test_user_id"]
    TEST_CONTENT_ID = submission_data["test_content_id"]

USERS = [TEST_USER_ID, "user_001", "user_002", "user_003"]
CONTENTS = [TEST_CONTENT_ID, "content_001", "content_002", "content_003"]
CATEGORIES = ["sci-fi", "education", "comedy", "news"]
EVENT_TYPES = ["view", "like", "share", "click"]

def setup_kafka_topics():
    """Creates topics with specific configurations (compaction)."""
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    existing_topics = admin_client.list_topics()

    topics_to_create = []
    
    if TOPIC_USER_EVENTS not in existing_topics:
        # Standard topic, 3 partitions as required
        topics_to_create.append(NewTopic(name=TOPIC_USER_EVENTS, num_partitions=3, replication_factor=1))
        
    if TOPIC_CONTENT_METADATA not in existing_topics:
        # Compacted topic
        topics_to_create.append(NewTopic(
            name=TOPIC_CONTENT_METADATA, num_partitions=1, replication_factor=1,
            topic_configs={'cleanup.policy': 'compact'}
        ))
        
    if TOPIC_FEATURE_STORE not in existing_topics:
        # Compacted topic
        topics_to_create.append(NewTopic(
            name=TOPIC_FEATURE_STORE, num_partitions=1, replication_factor=1,
            topic_configs={'cleanup.policy': 'compact'}
        ))

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create)
        print(f"Created topics: {[t.name for t in topics_to_create]}")
    else:
        print("Topics already exist.")
    admin_client.close()

def publish_metadata(producer):
    """Publish initial lookup data to the compacted metadata topic."""
    for content_id in CONTENTS:
        metadata = {
            "content_id": content_id,
            "category": random.choice(CATEGORIES),
            "creator_id": f"creator_{random.randint(1, 10)}",
            "publish_timestamp": datetime.utcnow().isoformat() + "Z"
        }
        producer.send(TOPIC_CONTENT_METADATA, key=content_id.encode('utf-8'), value=metadata)
    producer.flush()
    print("Published content metadata.")

def generate_user_events(producer):
    """Simulate continuous user activity with 5% late events."""
    print("Starting continuous event generation...")
    while True:
        # Calculate event timestamp
        event_time = datetime.utcnow()
        
        # 5% chance to create a deliberately late event (35-90 seconds in the past)
        if random.random() < 0.05:
            delay_seconds = random.randint(35, 90)
            event_time = event_time - timedelta(seconds=delay_seconds)
            print(f"Produced LATE event (delayed by {delay_seconds}s)")

        event = {
            "user_id": random.choice(USERS),
            "content_id": random.choice(CONTENTS),
            "event_type": random.choice(EVENT_TYPES),
            "dwell_time_ms": random.randint(1000, 120000),
            "timestamp": event_time.isoformat() + "Z"
        }
        
        producer.send(TOPIC_USER_EVENTS, value=event)
        
        # Accelerated time: Sleep briefly between events
        time.sleep(random.uniform(0.1, 0.5))

if __name__ == "__main__":
    # Wait for Kafka to be ready
    time.sleep(15) 
    
    setup_kafka_topics()
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    publish_metadata(producer)
    generate_user_events(producer)