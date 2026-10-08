from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from models.enums import Severity

AlertState = Literal["OPEN", "ACKNOWLEDGED", "RESOLVED"]


class AlertOut(BaseModel):
    """The spec's Alert Model, plus lifecycle bookkeeping. Built from a db.alerts.Alert row."""

    model_config = ConfigDict(from_attributes=True)

    alert_id: str
    rule_id: str
    rule_name: str
    state: AlertState
    severity: Severity
    frequency_mhz: float  # center of the signal bucket
    sensor_ids: list[str]
    first_seen: datetime  # event time of the first contributing observation
    last_seen: datetime   # event time of the newest contributing observation
    occurrences: int
    peak_power_dbm: float
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    resolved_by: Literal["auto", "manual"] | None = None


class AlertObservationOut(BaseModel):
    """One observation that contributed to an alert. Built from a db.alerts.AlertObservation row."""

    model_config = ConfigDict(from_attributes=True)

    observation_id: int  # id in ingest_db.observations
    sensor_id: str
    timestamp: datetime
    frequency_mhz: float
    power_dbm: float


class AlertDetail(AlertOut):
    observations: list[AlertObservationOut]
