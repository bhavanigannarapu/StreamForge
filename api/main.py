import os
import json
import time
import random
import threading
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=background_kafka_or_simulator, daemon=True)
    t.start()
    yield

# Initialize FastAPI App
app = FastAPI(
    title="StreamForge Backend API",
    description="Distributed Event Processor API for IoT Fleet Telemetry",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for Frontend Communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared In-Memory Data Store (Buffers latest readings & state)
START_TIME = datetime.now(timezone.utc)
telemetry_buffer: List[Dict[str, Any]] = []
truck_state: Dict[str, Dict[str, Any]] = {}
dlq_buffer: List[Dict[str, Any]] = []
MAX_BUFFER_SIZE = 500

TRUCK_IDS = [f"TRUCK-{i:03d}" for i in range(1, 11)]


class TelemetryItem(BaseModel):
    event_id: str = Field(..., description="Unique UUID for event deduplication")
    truck_id: str = Field(..., description="Truck Identifier e.g. TRUCK-001")
    temperature: float = Field(..., description="Sensor temperature reading in Celsius")
    speed: float = Field(..., description="Truck speed in km/h")
    timestamp: str = Field(..., description="ISO 8601 Timestamp")


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    uptime_seconds: float
    timestamp: str
    kafka_connected: bool
    total_events_processed: int


def generate_mock_telemetry_event() -> Dict[str, Any]:
    """Generates a realistic IoT truck telemetry reading."""
    truck_id = random.choice(TRUCK_IDS)
    # Occasionally generate invalid temperatures (< -5°C) to demonstrate DLQ routing
    is_invalid = random.random() < 0.08
    temp = round(random.uniform(-15.0, -6.0), 2) if is_invalid else round(random.uniform(5.0, 38.0), 2)
    
    return {
        "event_id": f"evt-{int(time.time()*1000)}-{random.randint(1000, 9999)}",
        "truck_id": truck_id,
        "temperature": temp,
        "speed": round(random.uniform(40.0, 95.0), 1),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def process_telemetry_record(record: Dict[str, Any]):
    """Processes incoming telemetry event and updates local state."""
    global telemetry_buffer, truck_state, dlq_buffer

    # DLQ Filter Rule (Temperature < -5°C or invalid schema)
    if record.get("temperature", 0) < -5.0:
        record["dlq_reason"] = f"Temperature out of valid range ({record.get('temperature')}°C < -5°C)"
        dlq_buffer.append(record)
        if len(dlq_buffer) > MAX_BUFFER_SIZE:
            dlq_buffer.pop(0)
        return

    # Add to telemetry buffer
    telemetry_buffer.append(record)
    if len(telemetry_buffer) > MAX_BUFFER_SIZE:
        telemetry_buffer.pop(0)

    # Update State Management
    truck_id = record["truck_id"]
    temp = record["temperature"]

    if truck_id not in truck_state:
        truck_state[truck_id] = {
            "truck_id": truck_id,
            "last_temperature": temp,
            "last_speed": record["speed"],
            "last_seen": record["timestamp"],
            "event_count": 1,
            "temp_sum": temp,
            "avg_temperature": temp,
            "max_temperature": temp,
            "min_temperature": temp
        }
    else:
        st = truck_state[truck_id]
        st["last_temperature"] = temp
        st["last_speed"] = record["speed"]
        st["last_seen"] = record["timestamp"]
        st["event_count"] += 1
        st["temp_sum"] += temp
        st["avg_temperature"] = round(st["temp_sum"] / st["event_count"], 2)
        st["max_temperature"] = max(st["max_temperature"], temp)
        st["min_temperature"] = min(st["min_temperature"], temp)


kafka_connected_flag = False

def background_kafka_or_simulator():
    """Attempts Kafka connection; falls back to simulator if broker is unavailable."""
    global kafka_connected_flag
    
    # Try connecting to Kafka
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            'truck-telemetry',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000
        )
        kafka_connected_flag = True
        print(" Connected to Kafka Broker on localhost:9092")
        while True:
            for message in consumer:
                process_telemetry_record(message.value)
            time.sleep(0.1)
    except Exception as e:
        kafka_connected_flag = False
        print(f"⚡ Kafka connection not available ({e}). Running in resilient standalone simulation mode...")
        # Populate initial historical data
        for _ in range(25):
            process_telemetry_record(generate_mock_telemetry_event())
        
        while True:
            process_telemetry_record(generate_mock_telemetry_event())
            time.sleep(1.5)


# Root Endpoint
@app.get("/")
def read_root():
    return {
        "message": "Welcome to StreamForge Distributed Telemetry API",
        "documentation": "/docs",
        "health_check": "/health",
        "telemetry": "/telemetry"
    }


# Health Endpoint (Required by Task & Architecture)
@app.get("/health", response_model=HealthResponse)
def get_health():
    uptime = (datetime.now(timezone.utc) - START_TIME).total_seconds()
    return {
        "status": "ONLINE",
        "service": "StreamForge Backend API",
        "version": "1.0.0",
        "uptime_seconds": round(uptime, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "kafka_connected": kafka_connected_flag,
        "total_events_processed": len(telemetry_buffer) + len(dlq_buffer)
    }


# Telemetry Endpoint (Primary)
@app.get("/telemetry")
def get_telemetry(
    limit: int = Query(50, ge=1, le=500, description="Max telemetry records to return"),
    truck_id: Optional[str] = Query(None, description="Filter by truck ID"),
    min_temp: Optional[float] = Query(None, description="Minimum temperature filter"),
    max_temp: Optional[float] = Query(None, description="Maximum temperature filter")
):
    results = telemetry_buffer.copy()
    
    if truck_id:
        results = [r for r in results if r["truck_id"].upper() == truck_id.upper()]
    if min_temp is not None:
        results = [r for r in results if r["temperature"] >= min_temp]
    if max_temp is not None:
        results = [r for r in results if r["temperature"] <= max_temp]
        
    return {
        "count": len(results),
        "data": results[-limit:]
    }


# Telemetre Endpoint (Alias for exact compatibility with prompt GET /telemetre)
@app.get("/telemetre")
def get_telemetre(
    limit: int = Query(50, ge=1, le=500),
    truck_id: Optional[str] = None
):
    return get_telemetry(limit=limit, truck_id=truck_id)


# Single Truck Telemetry Endpoint
@app.get("/telemetry/{truck_id}")
def get_truck_telemetry(truck_id: str, limit: int = Query(20, ge=1, le=100)):
    truck_records = [r for r in telemetry_buffer if r["truck_id"].upper() == truck_id.upper()]
    if not truck_records:
        raise HTTPException(status_code=404, detail=f"No telemetry found for truck {truck_id}")
    
    return {
        "truck_id": truck_id.upper(),
        "count": len(truck_records),
        "state_summary": truck_state.get(truck_id.upper(), {}),
        "history": truck_records[-limit:]
    }


# System Metrics Endpoint
@app.get("/metrics")
def get_metrics():
    if not telemetry_buffer:
        return {
            "total_events": 0,
            "active_trucks": 0,
            "avg_fleet_temperature": 0.0,
            "avg_fleet_speed": 0.0
        }
    
    temps = [r["temperature"] for r in telemetry_buffer]
    speeds = [r["speed"] for r in telemetry_buffer]
    
    return {
        "total_events": len(telemetry_buffer),
        "dlq_events_count": len(dlq_buffer),
        "active_trucks": len(truck_state),
        "avg_fleet_temperature": round(sum(temps) / len(temps), 2),
        "min_fleet_temperature": round(min(temps), 2),
        "max_fleet_temperature": round(max(temps), 2),
        "avg_fleet_speed": round(sum(speeds) / len(speeds), 1),
        "truck_ids": list(truck_state.keys())
    }


# State Store Endpoint (RocksDB / In-Memory State)
@app.get("/state")
def get_state():
    return {
        "count": len(truck_state),
        "truck_state": truck_state
    }


# DLQ Endpoint (Dead Letter Queue)
@app.get("/dlq")
def get_dlq(limit: int = Query(50, ge=1, le=200)):
    return {
        "count": len(dlq_buffer),
        "dlq_records": dlq_buffer[-limit:]
    }


# Ingest Telemetry Manually (POST Endpoint)
@app.post("/telemetry", status_code=201)
def post_telemetry(event: TelemetryItem):
    record = event.model_dump()
    process_telemetry_record(record)
    return {
        "status": "ACCEPTED",
        "event_id": record["event_id"],
        "truck_id": record["truck_id"]
    }
