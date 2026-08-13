import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class AlertRule(BaseModel):
    rule_id: str
    metric: str  # "temperature" or "speed"
    condition: str  # ">", "<", ">=", "<="
    threshold: float
    severity: str  # "CRITICAL", "WARNING", "INFO"
    message_template: str


class AlertEvent(BaseModel):
    alert_id: str
    rule_id: str
    truck_id: str
    metric: str
    value: float
    threshold: float
    severity: str
    message: str
    timestamp: str


class AlertEngine:
    """Real-time Threshold & Anomaly Monitoring Engine."""
    
    def __init__(self):
        self.rules: List[AlertRule] = [
            AlertRule(
                rule_id="RULE-TEMP-CRIT",
                metric="temperature",
                condition=">",
                threshold=35.0,
                severity="CRITICAL",
                message_template="Overheat Alert! Truck {truck_id} temperature reached {value}°C (Threshold: >{threshold}°C)"
            ),
            AlertRule(
                rule_id="RULE-TEMP-WARN-LOW",
                metric="temperature",
                condition="<",
                threshold=0.0,
                severity="WARNING",
                message_template="Freezing Warning! Truck {truck_id} temperature dropped to {value}°C (Threshold: <{threshold}°C)"
            ),
            AlertRule(
                rule_id="RULE-SPEED-WARN",
                metric="speed",
                condition=">",
                threshold=90.0,
                severity="WARNING",
                message_template="Overspeed Warning! Truck {truck_id} speed recorded at {value} km/h (Threshold: >{threshold} km/h)"
            )
        ]
        self.alerts_history: List[Dict[str, Any]] = []
        self.max_history = 200

    def evaluate_telemetry(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluates telemetry record against all alert rules."""
        triggered: List[Dict[str, Any]] = []
        truck_id = record.get("truck_id", "UNKNOWN")
        temp = record.get("temperature")
        speed = record.get("speed")

        for rule in self.rules:
            value = temp if rule.metric == "temperature" else speed
            if value is None:
                continue

            is_alert = False
            if rule.condition == ">" and value > rule.threshold:
                is_alert = True
            elif rule.condition == "<" and value < rule.threshold:
                is_alert = True
            elif rule.condition == ">=" and value >= rule.threshold:
                is_alert = True
            elif rule.condition == "<=" and value <= rule.threshold:
                is_alert = True

            if is_alert:
                alert_evt = {
                    "alert_id": f"alt-{int(time.time()*1000)}-{truck_id}",
                    "rule_id": rule.rule_id,
                    "truck_id": truck_id,
                    "metric": rule.metric,
                    "value": value,
                    "threshold": rule.threshold,
                    "severity": rule.severity,
                    "message": rule.message_template.format(truck_id=truck_id, value=value, threshold=rule.threshold),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                triggered.append(alert_evt)
                self.alerts_history.append(alert_evt)
                if len(self.alerts_history) > self.max_history:
                    self.alerts_history.pop(0)

        return triggered

    def get_alerts(self, limit: int = 50, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        alerts = self.alerts_history.copy()
        if severity:
            alerts = [a for a in alerts if a["severity"].upper() == severity.upper()]
        return alerts[-limit:]

    def clear_alerts(self):
        self.alerts_history.clear()


# Global Alert Engine Instance
alert_engine = AlertEngine()
