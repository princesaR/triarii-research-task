"""The message contract between the services (ingest -> Kafka -> alerts)."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from models.enums import Severity

TOPIC = "rf.rule-matches"


class MatchedObservation(BaseModel):
    id: int
    sensor_id: str
    timestamp: datetime
    frequency_mhz: float
    power_dbm: float


class RuleMatchEvent(BaseModel):
    """Published by ingest when an observation satisfies a rule, including the
    min_duration persistence requirement."""

    event: Literal["rule_match"] = "rule_match"
    rule_id: str
    rule_name: str
    severity: Severity
    resolve_after_s: float
    signal_key: str
    frequency_mhz: float
    condition_start: datetime  # event time the condition started persisting
    observation: MatchedObservation

    def kafka_key(self) -> str:
        # Same key -> same partition, so all messages of one signal stay in order.
        return f"{self.rule_id}|{self.signal_key}"
