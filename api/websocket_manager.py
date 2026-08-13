import json
from typing import List
from fastapi import WebSocket


class ConnectionManager:
    """WebSockets Connection Manager for real-time streaming notifications."""

    def __init__(self):
        self.active_telemetry_connections: List[WebSocket] = []
        self.active_alert_connections: List[WebSocket] = []

    async def connect_telemetry(self, websocket: WebSocket):
        await websocket.accept()
        self.active_telemetry_connections.append(websocket)

    def disconnect_telemetry(self, websocket: WebSocket):
        if websocket in self.active_telemetry_connections:
            self.active_telemetry_connections.remove(websocket)

    async def connect_alerts(self, websocket: WebSocket):
        await websocket.accept()
        self.active_alert_connections.append(websocket)

    def disconnect_alerts(self, websocket: WebSocket):
        if websocket in self.active_alert_connections:
            self.active_alert_connections.remove(websocket)

    async def broadcast_telemetry(self, message: dict):
        disconnected = []
        for connection in self.active_telemetry_connections:
            try:
                await connection.send_text(json.dumps(message))
            except Exception:
                disconnected.append(connection)
        for conn in disconnected:
            self.disconnect_telemetry(conn)

    async def broadcast_alert(self, alert_data: dict):
        disconnected = []
        for connection in self.active_alert_connections:
            try:
                await connection.send_text(json.dumps(alert_data))
            except Exception:
                disconnected.append(connection)
        for conn in disconnected:
            self.disconnect_alerts(conn)


ws_manager = ConnectionManager()
