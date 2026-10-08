"""The steps of POST /api/v1/observations, kept out of the route so it reads as a recipe."""

from pydantic import ValidationError
from sqlalchemy.orm import Session

from db.ingest import Observation, Rule, Sensor
from ingest.engine import RuleEngine
from models import FieldError, MatchedObservation, ObservationIn, RejectedItem, RuleMatchEvent


def validate_batch(items: list) -> tuple[list[ObservationIn], list[RejectedItem]]:
    """Validate each item on its own: valid ones go on, invalid ones are reported by index."""
    valid, rejected = [], []
    for i, item in enumerate(items):
        try:
            valid.append(ObservationIn.model_validate(item))
        except ValidationError as e:
            rejected.append(RejectedItem(index=i, errors=[
                FieldError(field=".".join(str(p) for p in err["loc"]) or "body",
                           message=err["msg"].removeprefix("Value error, "))
                for err in e.errors(include_url=False)]))
    return valid, rejected


def store(s: Session, observations: list[ObservationIn]) -> list[Observation]:
    """Insert the observations and update sensors. Flushes (ids assigned), does not commit."""
    rows = [Observation(sensor_id=o.sensor_id, timestamp=o.timestamp,
                        frequency_mhz=o.frequency_mhz, bandwidth_khz=o.bandwidth_khz,
                        power_dbm=o.power_dbm, lat=o.location.lat, lon=o.location.lon)
            for o in observations]
    s.add_all(rows)
    _touch_sensors(s, rows)
    s.flush()
    return rows


def evaluate(engine: RuleEngine, rules: list[Rule],
             rows: list[Observation]) -> list[RuleMatchEvent]:
    """Run the rules over the rows in event-time order and build the Kafka messages."""
    events = []
    for row in sorted(rows, key=lambda r: r.timestamp):
        for m in engine.evaluate(row, rules):
            events.append(RuleMatchEvent(
                rule_id=m.rule.rule_id, rule_name=m.rule.name, severity=m.rule.severity,
                resolve_after_s=m.rule.resolve_after_s, signal_key=m.signal_key,
                frequency_mhz=float(m.signal_key), condition_start=m.condition_start,
                observation=MatchedObservation.model_validate(row, from_attributes=True)))
    return events


def _touch_sensors(s: Session, rows: list[Observation]) -> None:
    """Upsert each sensor's last_seen; out-of-order data never moves it back."""
    latest: dict[str, Observation] = {}
    for r in rows:
        if r.sensor_id not in latest or r.timestamp > latest[r.sensor_id].timestamp:
            latest[r.sensor_id] = r
    for sid, r in latest.items():
        sensor = s.get(Sensor, sid)
        if sensor is None:
            s.add(Sensor(sensor_id=sid, last_seen=r.timestamp, lat=r.lat, lon=r.lon))
        elif r.timestamp > sensor.last_seen:
            sensor.last_seen, sensor.lat, sensor.lon = r.timestamp, r.lat, r.lon
