from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "documentation" in data
    assert "health_check" in data

def test_get_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ONLINE"
    assert data["service"] == "StreamForge Backend API"
    assert "uptime_seconds" in data
    assert "timestamp" in data

def test_cors_headers():
    response = client.get("/health", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") in ["*", "http://localhost:3000"]

def test_get_telemetry():
    response = client.get("/telemetry?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert "data" in data
    assert isinstance(data["data"], list)

def test_get_telemetre_alias():
    response = client.get("/telemetre?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert "data" in data

def test_get_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "total_events" in data
    assert "active_trucks" in data
    assert "avg_fleet_temperature" in data

def test_get_state():
    response = client.get("/state")
    assert response.status_code == 200
    data = response.json()
    assert "truck_state" in data

def test_get_dlq():
    response = client.get("/dlq")
    assert response.status_code == 200
    data = response.json()
    assert "dlq_records" in data

def test_post_telemetry():
    payload = {
        "event_id": "test-uuid-999",
        "truck_id": "TRUCK-099",
        "temperature": 24.5,
        "speed": 65.0,
        "timestamp": "2026-08-01T15:00:00Z"
    }
    response = client.post("/telemetry", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "ACCEPTED"
    assert data["truck_id"] == "TRUCK-099"
