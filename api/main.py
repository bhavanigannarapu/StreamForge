import os
import json
import time
import random
import threading
import io
import csv
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from contextlib import asynccontextmanager

from api.alerts import alert_engine, AlertRule
from api.websocket_manager import ws_manager
from api.middleware import RateLimitMiddleware

# Shared In-Memory Data Store (Buffers latest readings & state)
START_TIME = datetime.now(timezone.utc)
telemetry_buffer: List[Dict[str, Any]] = []
truck_state: Dict[str, Dict[str, Any]] = {}
dlq_buffer: List[Dict[str, Any]] = []
MAX_BUFFER_SIZE = 500

TRUCK_IDS = [f"TRUCK-{i:03d}" for i in range(1, 11)]

# Simulated GPS Fleet Base Coordinates (Center: Chicago/Midwest Route Grid)
BASE_COORDS = {
    f"TRUCK-{i:03d}": {"lat": 41.8781 + (i * 0.08), "lon": -87.6298 + (i * 0.06)}
    for i in range(1, 11)
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=background_kafka_or_simulator, daemon=True)
    t.start()
    yield


# Initialize FastAPI App
app = FastAPI(
    title="StreamForge Distributed Telemetry API",
    description="Distributed Event Processor API for IoT Fleet Telemetry, Prometheus Observability & WebSockets",
    version="2.0.0",
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


class TelemetryItem(BaseModel):
    event_id: str = Field(..., description="Unique UUID for event deduplication")
    truck_id: str = Field(..., description="Truck Identifier e.g. TRUCK-001")
    temperature: float = Field(..., description="Sensor temperature reading in Celsius")
    speed: float = Field(..., description="Truck speed in km/h")
    timestamp: str = Field(..., description="ISO 8601 Timestamp")
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    uptime_seconds: float
    timestamp: str
    kafka_connected: bool
    total_events_processed: int
    active_alerts_count: int


def generate_mock_telemetry_event() -> Dict[str, Any]:
    """Generates a realistic IoT truck telemetry reading with GPS coordinates."""
    truck_id = random.choice(TRUCK_IDS)
    # Occasionally generate invalid temperatures (< -5°C) to demonstrate DLQ routing
    is_invalid = random.random() < 0.08
    temp = round(random.uniform(-15.0, -6.0), 2) if is_invalid else round(random.uniform(5.0, 38.0), 2)
    
    # Slight GPS jitter for movement simulation
    base = BASE_COORDS[truck_id]
    lat = round(base["lat"] + random.uniform(-0.02, 0.02), 4)
    lon = round(base["lon"] + random.uniform(-0.02, 0.02), 4)
    BASE_COORDS[truck_id] = {"lat": lat, "lon": lon}

    return {
        "event_id": f"evt-{int(time.time()*1000)}-{random.randint(1000, 9999)}",
        "truck_id": truck_id,
        "temperature": temp,
        "speed": round(random.uniform(40.0, 95.0), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": lat,
        "longitude": lon
    }


def process_telemetry_record(record: Dict[str, Any]):
    """Processes incoming telemetry event, checks alert rules, and updates local state."""
    global telemetry_buffer, truck_state, dlq_buffer

    # DLQ Filter Rule (Temperature < -5°C or invalid schema)
    if record.get("temperature", 0) < -5.0:
        record["dlq_reason"] = f"Temperature out of valid boundary ({record.get('temperature')}°C < -5°C)"
        dlq_buffer.append(record)
        if len(dlq_buffer) > MAX_BUFFER_SIZE:
            dlq_buffer.pop(0)
        return

    # Add to telemetry buffer
    telemetry_buffer.append(record)
    if len(telemetry_buffer) > MAX_BUFFER_SIZE:
        telemetry_buffer.pop(0)

    # Evaluate Anomaly Alert Rules
    alert_engine.evaluate_telemetry(record)

    # Update State Management
    truck_id = record["truck_id"]
    temp = record["temperature"]

    if truck_id not in truck_state:
        truck_state[truck_id] = {
            "truck_id": truck_id,
            "last_temperature": temp,
            "last_speed": record["speed"],
            "latitude": record.get("latitude", 41.8781),
            "longitude": record.get("longitude", -87.6298),
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
        st["latitude"] = record.get("latitude", st.get("latitude"))
        st["longitude"] = record.get("longitude", st.get("longitude"))
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
        for _ in range(35):
            process_telemetry_record(generate_mock_telemetry_event())
        
        while True:
            process_telemetry_record(generate_mock_telemetry_event())
            time.sleep(1.5)


# Root Endpoint
@app.get("/")
def read_root():
    return {
        "message": "Welcome to StreamForge Distributed Telemetry API v2.0",
        "documentation": "/docs",
        "health_check": "/health",
        "telemetry": "/telemetry",
        "prometheus_metrics": "/prometheus",
        "websocket_stream": "/ws/telemetry"
    }


# Health Endpoint
@app.get("/health", response_model=HealthResponse)
def get_health():
    uptime = (datetime.now(timezone.utc) - START_TIME).total_seconds()
    return {
        "status": "ONLINE",
        "service": "StreamForge Backend API",
        "version": "2.0.0",
        "uptime_seconds": round(uptime, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "kafka_connected": kafka_connected_flag,
        "total_events_processed": len(telemetry_buffer) + len(dlq_buffer),
        "active_alerts_count": len(alert_engine.get_alerts())
    }


# Telemetry Endpoint
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


# Telemetre Alias Endpoint
@app.get("/telemetre")
def get_telemetre(
    limit: int = Query(50, ge=1, le=500),
    truck_id: Optional[str] = None
):
    return get_telemetry(limit=limit, truck_id=truck_id)


# Telemetry Export Endpoint (CSV / JSON) - Declared before path param route
@app.get("/telemetry/export")
def export_telemetry(format: str = Query("csv", pattern="^(csv|json)$")):
    if format == "json":
        return telemetry_buffer

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["event_id", "truck_id", "temperature", "speed", "timestamp", "latitude", "longitude"]
    )
    writer.writeheader()
    for row in telemetry_buffer:
        filtered_row = {k: row.get(k, "") for k in writer.fieldnames}
        writer.writerow(filtered_row)

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=streamforge_telemetry_{int(time.time())}.csv"}
    )


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
        "active_alerts_count": len(alert_engine.get_alerts()),
        "truck_ids": list(truck_state.keys())
    }


# Prometheus Standard Metrics Endpoint
@app.get("/prometheus")
def get_prometheus_metrics():
    uptime = (datetime.now(timezone.utc) - START_TIME).total_seconds()
    temps = [r["temperature"] for r in telemetry_buffer] if telemetry_buffer else [0.0]
    avg_temp = round(sum(temps) / len(temps), 2) if temps else 0.0
    kafka_val = 1 if kafka_connected_flag else 0
    
    metrics_str = f"""# HELP streamforge_uptime_seconds Total uptime of StreamForge API in seconds
# TYPE streamforge_uptime_seconds gauge
streamforge_uptime_seconds {uptime}

# HELP streamforge_telemetry_events_total Total telemetry events in memory buffer
# TYPE streamforge_telemetry_events_total counter
streamforge_telemetry_events_total {len(telemetry_buffer)}

# HELP streamforge_dlq_events_total Total dead-letter queue events intercepted
# TYPE streamforge_dlq_events_total counter
streamforge_dlq_events_total {len(dlq_buffer)}

# HELP streamforge_active_trucks Number of active fleet trucks
# TYPE streamforge_active_trucks gauge
streamforge_active_trucks {len(truck_state)}

# HELP streamforge_avg_temperature_celsius Average temperature across active fleet
# TYPE streamforge_avg_temperature_celsius gauge
streamforge_avg_temperature_celsius {avg_temp}

# HELP streamforge_kafka_connected Kafka broker connection status (1=Connected, 0=Simulation)
# TYPE streamforge_kafka_connected gauge
streamforge_kafka_connected {kafka_val}

# HELP streamforge_alerts_total Total active anomaly alerts triggered
# TYPE streamforge_alerts_total counter
streamforge_alerts_total {len(alert_engine.get_alerts())}
"""
    return Response(content=metrics_str, media_type="text/plain")


# Alert Endpoints
@app.get("/alerts")
def get_alerts(limit: int = Query(50, ge=1, le=200), severity: Optional[str] = None):
    return {
        "count": len(alert_engine.get_alerts(limit=limit, severity=severity)),
        "alerts": alert_engine.get_alerts(limit=limit, severity=severity)
    }


@app.post("/alerts/clear")
def clear_alerts():
    alert_engine.clear_alerts()
    return {"status": "SUCCESS", "message": "Alert history cleared."}


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


# WebSockets Stream Endpoints
@app.websocket("/ws/telemetry")
async def websocket_telemetry_endpoint(websocket: WebSocket):
    await ws_manager.connect_telemetry(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect_telemetry(websocket)
