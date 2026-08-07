import json
from kafka import KafkaConsumer
from rocksdict import Rdict

# Create/Open RocksDB database
db = Rdict("./rocksdb_data")

# Kafka Consumer
consumer = KafkaConsumer(
    "clean-telemetry",  # Change if your Kafka topic is different
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("State Manager started... Waiting for messages...")

for message in consumer:
    data = message.value

    truck_id = str(data["truck_id"])
    temperature = float(data["temperature"])

    # Read previous state
    if truck_id in db:
        state = json.loads(db[truck_id])
    else:
        state = {
            "latest_temperature": 0.0,
            "count": 0,
            "sum": 0.0,
            "average": 0.0
        }

    # Update state
    state["latest_temperature"] = temperature
    state["count"] += 1
    state["sum"] += temperature
    state["average"] = state["sum"] / state["count"]

    # Save updated state
    db[truck_id] = json.dumps(state)

    print(
        f"Truck ID: {truck_id} | "
        f"Latest Temp: {state['latest_temperature']}°C | "
        f"Average Temp: {state['average']:.2f}°C"
    )