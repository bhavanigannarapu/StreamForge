from kafka import KafkaConsumer
import json


TOPIC_NAME = "truck-telemetry"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest"
)


processed_events = set()


print("🚀 Worker started and waiting for events...")


for message in consumer:

    event = message.value

    event_id = event["event_id"]

    # Duplicate data checking
    if event_id in processed_events:
        print("Duplicate event detected:", event_id)
        continue

    processed_events.add(event_id)


    # Error checking
    if event["temperature"] < -5:
        print("Invalid temperature detected:", event)
        continue


    print("✅ Processed Event:")
    print(event)