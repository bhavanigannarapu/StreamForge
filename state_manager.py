import json
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer
from rocksdict import Rdict

# Configuration
KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'clean-telemetry'
CHANGELOG_TOPIC = 'state-changelog'
ROCKSDB_PATH = './rocksdb_data'

# 1. Initialize RocksDB Embedded State Store
db = Rdict(ROCKSDB_PATH)

# 2. Initialize Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='state-manager-group'
)

# 3. Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

print(f"⚡ State Manager Engine Running...")
print(f"   Listening on topic: '{INPUT_TOPIC}'")
print(f"   State store location: '{ROCKSDB_PATH}'\n")

# Graceful shutdown handler to safely release RocksDB lock files
def shutdown(sig, frame):
    print("\nShutting down gracefully...")
    consumer.close()
    producer.flush()
    producer.close()
    db.close()
    print("RocksDB state store saved and closed safely.")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)

try:
    for message in consumer:
        event = message.value
        truck_id = str(event.get("truck_id"))
        temperature = event.get("temperature")

        # Guard clause for invalid payloads
        if not truck_id or temperature is None:
            continue

        # Fetch current state from RocksDB or initialize default structure
        current_state = db.get(truck_id, {
            "truck_id": truck_id,
            "total_events": 0,
            "temp_sum": 0.0,
            "avg_temp": 0.0,
            "max_temp": float('-inf'),
            "last_speed": 0.0,
            "last_timestamp": event.get("timestamp")
        })

        # State Calculations
        current_state["total_events"] += 1
        current_state["temp_sum"] += float(temperature)
        current_state["avg_temp"] = round(current_state["temp_sum"] / current_state["total_events"], 2)
        current_state["max_temp"] = max(current_state["max_temp"], float(temperature))
        current_state["last_speed"] = event.get("speed", 0.0)
        current_state["last_timestamp"] = event.get("timestamp")

        # Save to RocksDB persistence
        db[truck_id] = current_state

        # Emit state update event to changelog topic for downstream UI/API
        producer.send(CHANGELOG_TOPIC, key=truck_id, value=current_state)

        print(
            f"[STATE UPDATED] Truck ID: {truck_id} | "
            f"Avg Temp: {current_state['avg_temp']}°C | "
            f"Events Processed: {current_state['total_events']}"
        )

except Exception as e:
    print(f"Error in state processing loop: {e}")
finally:
    db.close()