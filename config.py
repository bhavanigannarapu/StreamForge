import os

# Kafka Cluster Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Topic Specifications
TOPIC_RAW_TELEMETRY = "truck-telemetry"
TOPIC_CLEAN_TELEMETRY = "clean-telemetry"
TOPIC_STATE_CHANGELOG = "state-changelog"
TOPIC_ALERTS = "telemetry-alerts"
TOPIC_DLQ = "dead-letter-queue"

# Alert Engine Thresholds
TEMP_THRESHOLD_CELSIUS = 80.0
SPEED_THRESHOLD_MPH = 85.0

# Database Configuration
SQLITE_DB_PATH = "streamforge_history.db"