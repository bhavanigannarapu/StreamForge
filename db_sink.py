import json
import sqlite3
from kafka import KafkaConsumer

DB_FILE = "streamforge_history.db"

# Initialize Database
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    truck_id TEXT,
    timestamp TEXT,
    temperature REAL,
    speed REAL
)
""")

conn.commit()

# Kafka Consumer
consumer = KafkaConsumer(
    "clean-telemetry",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="db-sink-group"
)

print(f"🗄️ Database Sink Active! Saving records to '{DB_FILE}'...")

try:
    for message in consumer:
        event = message.value

        cursor.execute("""
            INSERT INTO telemetry (truck_id, timestamp, temperature, speed)
            VALUES (?, ?, ?, ?)
        """, (
            event.get("truck_id"),
            event.get("timestamp"),
            event.get("temperature"),
            event.get("speed")
        ))

        conn.commit()

        print(f"💾 [SAVED] Archived event for {event.get('truck_id')}")

except KeyboardInterrupt:
    print("\nShutting down Database Sink...")

finally:
    consumer.close()
    conn.close()