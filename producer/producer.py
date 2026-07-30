import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import uuid

# Connect to Kafka running via Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')  # Serializes Kafka routing key
)

TOPIC_NAME = 'truck-telemetry'
TRUCK_IDS = [f"TRUCK-{i:03d}" for i in range(1, 11)]  # TRUCK-001 to TRUCK-010

def generate_telemetry():
    truck_id = random.choice(TRUCK_IDS)
    return {
        "event_id": str(uuid.uuid4()),        # Crucial for deduplication & exactly-once processing
        "truck_id": truck_id,
        "temperature": round(random.uniform(-5.0, 35.0), 2),  # Includes negative values for edge-case filtering
        "speed": round(random.uniform(40.0, 90.0), 1),
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    print(f"🚀 Starting Data Producer... Sending events to topic '{TOPIC_NAME}'")
    try:
        while True:
            data = generate_telemetry()
            # Passing key=data["truck_id"] ensures all events for TRUCK-001 always go to the same Kafka partition
            producer.send(TOPIC_NAME, key=data["truck_id"], value=data)
            print(f"[SENT]: {data}")
            time.sleep(1)  # Sends 1 message every second
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.flush()
        producer.close()
        print("Producer stopped cleanly.")