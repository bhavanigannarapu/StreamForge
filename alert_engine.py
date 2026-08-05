import json
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'clean-telemetry'
ALERT_TOPIC = 'telemetry-alerts'

# Threshold Rules
TEMP_THRESHOLD = 80.0    # °C
SPEED_THRESHOLD = 85.0   # mph

# 1. Initialize Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='alert-engine-group'
)

# 2. Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

print(f"🚨 Real-Time Alert Engine Running...")
print(f"   Listening on: '{INPUT_TOPIC}'")
print(f"   Emitting alerts to: '{ALERT_TOPIC}'\n")

# Graceful shutdown handler
def shutdown(sig, frame):
    print("\nShutting down Alert Engine gracefully...")
    consumer.close()
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)

try:
    for message in consumer:
        event = message.value

        if not isinstance(event, dict):
            continue

        truck_id = str(event.get("truck_id", "UNKNOWN"))
        temp = float(event.get("temperature", 0.0))
        speed = float(event.get("speed", 0.0))
        timestamp = event.get("timestamp")

        alerts = []

        # Rule 1: Engine Overheat Check
        if temp > TEMP_THRESHOLD:
            alerts.append({
                "alert_type": "OVERHEAT_WARNING",
                "severity": "CRITICAL",
                "value": temp,
                "unit": "°C",
                "message": f"Engine temp exceeded threshold ({temp}°C > {TEMP_THRESHOLD}°C)"
            })

        # Rule 2: Speeding Violation Check
        if speed > SPEED_THRESHOLD:
            alerts.append({
                "alert_type": "SPEED_VIOLATION",
                "severity": "HIGH",
                "value": speed,
                "unit": "mph",
                "message": f"Vehicle speed exceeded limit ({speed}mph > {SPEED_THRESHOLD}mph)"
            })

        # Emit all triggered alerts to telemetry-alerts topic
        for alert in alerts:
            alert_payload = {
                "truck_id": truck_id,
                "timestamp": timestamp,
                "alert_type": alert["alert_type"],
                "severity": alert["severity"],
                "value": alert["value"],
                "details": alert["message"]
            }

            producer.send(ALERT_TOPIC, key=truck_id, value=alert_payload)
            print(f"🔥 [ALERT DETECTED] Truck {truck_id} | {alert['alert_type']} ({alert['severity']}) -> {alert['message']}")

except Exception as e:
    print(f"Error in Alert Engine processing loop: {e}")
finally:
    consumer.close()
    producer.flush()
    producer.close()