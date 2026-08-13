import json
import signal
import sys
from collections import deque
from kafka import KafkaConsumer, KafkaProducer

# Topic Configuration
INPUT_TOPIC = "truck-telemetry"
OUTPUT_TOPIC = "clean-telemetry"
KAFKA_BROKER = "localhost:9092"

# 1. Kafka Consumer (Listens for raw truck data)
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="telemetry-cleaner-group"
)

# 2. Kafka Producer (Pushes clean data to state_manager.py)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 3. Bounded Deduplication Cache (Prevents RAM Memory Leaks)
MAX_CACHE_SIZE = 10000
processed_events = set()
event_order = deque()

print("🚀 Telemetry Cleaner Engine Running...")
print(f"   Reading raw topic: '{INPUT_TOPIC}'")
print(f"   Writing clean topic: '{OUTPUT_TOPIC}'\n")

# Graceful exit handler
def shutdown(sig, frame):
    print("\nShutting down gracefully...")
    consumer.close()
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)

try:
    for message in consumer:
        event = message.value

        # Guard clause: Ignore non-dictionary or corrupted payloads
        if not isinstance(event, dict):
            continue

        event_id = event.get("event_id")
        temperature = event.get("temperature")

        # Guard clause: Ensure required fields exist
        if not event_id or temperature is None:
            continue

        # Bounded Deduplication check
        if event_id in processed_events:
            print(f"⚠️ Duplicate event skipped: {event_id}")
            continue

        # Add event to bounded memory cache
        processed_events.add(event_id)
        event_order.append(event_id)
        if len(event_order) > MAX_CACHE_SIZE:
            oldest_id = event_order.popleft()
            processed_events.remove(oldest_id)

        # Temperature Noise Filter
        if temperature < -5:
            print(f"⚠️ Invalid temperature filtered ({temperature}°C): {event_id}")
            continue

        # Forward clean event to clean-telemetry topic
        producer.send(OUTPUT_TOPIC, value=event)
        print(f"✅ Clean Event Forwarded -> {OUTPUT_TOPIC} | Event ID: {event_id}")

except Exception as e:
    print(f"Error in processing loop: {e}")
finally:
    consumer.close()
    producer.flush()
    producer.close()