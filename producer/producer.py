import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Connect to Kafka running via Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'truck-telemetry'
TRUCK_IDS = [f"TRUCK-{i:03d}" for i in range(1, 11)]  # TRUCK-001 to TRUCK-010

def generate_telemetry():
    return {
        "truck_id": random.choice(TRUCK_IDS),
        "temperature": round(random.uniform(18.0, 42.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print(f"🚀 Starting Data Producer... Sending events to topic '{TOPIC_NAME}'")
    try:
        while True:
            data = generate_telemetry()
            producer.send(TOPIC_NAME, value=data)
            print(f"[SENT]: {data}")
            time.sleep(1)  # Sends 1 message every second
    except KeyboardInterrupt:
        print("\nProducer stopped.")