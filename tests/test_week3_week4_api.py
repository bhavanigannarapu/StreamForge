from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_prometheus_endpoint():
    response = client.get("/prometheus")
    assert response.status_code == 200
    assert "streamforge_uptime_seconds" in response.text
    assert "streamforge_telemetry_events_total" in response.text
    assert "streamforge_active_trucks" in response.text
    assert "streamforge_alerts_total" in response.text
    assert response.headers["content-type"].startswith("text/plain")

def test_alerts_endpoint():
    response = client.get("/alerts")
    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert "alerts" in data
    assert isinstance(data["alerts"], list)

def test_alerts_clear():
    response = client.post("/alerts/clear")
    assert response.status_code == 200
    assert response.json()["status"] == "SUCCESS"

def test_telemetry_export_csv():
    response = client.get("/telemetry/export?format=csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "event_id,truck_id,temperature,speed" in response.text

def test_telemetry_export_json():
    response = client.get("/telemetry/export?format=json")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_health_v2():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["version"] == "2.0.0"
    assert "active_alerts_count" in data
