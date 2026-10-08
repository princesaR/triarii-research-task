"""Alert lifecycle: OPEN -> ACKNOWLEDGED -> RESOLVED. Pure DB logic, no Kafka, no HTTP.

- Deduplication: one active (OPEN/ACKNOWLEDGED) alert per (rule_id, signal_key).
  New matches update it; acknowledging does not stop updates.
- Auto-resolve: an active alert with no match for resolve_after_s is resolved.
  A match after resolution opens a new alert.
- Event time: first_seen/last_seen are min/max of observation timestamps, so
  out-of-order matches are folded in correctly.
- Late data:
  * a match newer than an active alert's expiry closes that alert (at its expiry
    time) and opens a new one, even if the sweeper has not run yet;
  * a match that belongs to an already-resolved alert is attached to it as
    evidence but never reopens it.
- Idempotency: (alert_id, observation_id) is unique, so a message Kafka delivers
  twice is counted once.
"""

import secrets
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.alerts import ACTIVE_STATES, Alert, AlertObservation
from models import RuleMatchEvent

Action = Literal["opened", "updated", "late", "duplicate"]


class InvalidTransition(Exception):
    pass


def new_alert_id() -> str:
    return f"alrt-{secrets.token_hex(4)}"


def apply_match(s: Session, ev: RuleMatchEvent) -> tuple[Alert, Action]:
    """Fold one rule match into the alerts table. The caller commits."""
    ts = ev.observation.timestamp
    active = _active_alert(s, ev.rule_id, ev.signal_key)

    if active is not None and _already_linked(s, active.alert_id, ev.observation.id):
        return active, "duplicate"

    # In event time this alert has already expired (the sweeper just has not run yet):
    # close it at the moment it expired, then treat this match as a new episode.
    if active is not None and ts > _expires_at(active):
        _resolve(active, _expires_at(active), "auto")
        s.flush()  # frees the partial unique index for the new alert
        active = None

    # A late match from inside an already-resolved alert's time span is evidence for
    # that alert, never a reason to reopen it (or to stretch a newer alert back in time).
    if active is None or ts < active.first_seen:
        past = _resolved_alert_covering(s, ev.rule_id, ev.signal_key, ts)
        if past is not None:
            if _already_linked(s, past.alert_id, ev.observation.id):
                return past, "duplicate"
            _link(s, past, ev)
            return past, "late"

    if active is not None:
        active.first_seen = min(active.first_seen, ts)
        active.last_seen = max(active.last_seen, ts)
        _link(s, active, ev)
        return active, "updated"

    alert = Alert(
        alert_id=new_alert_id(), rule_id=ev.rule_id, rule_name=ev.rule_name,
        severity=ev.severity, resolve_after_s=ev.resolve_after_s, signal_key=ev.signal_key,
        frequency_mhz=ev.frequency_mhz, state="OPEN", sensor_ids=[],
        first_seen=min(ev.condition_start, ts), last_seen=ts, occurrences=0,
        peak_power_dbm=ev.observation.power_dbm)
    s.add(alert)
    s.flush()
    _link(s, alert, ev)
    return alert, "opened"


def sweep_expired(s: Session, now: datetime) -> list[Alert]:
    """Auto-resolve active alerts whose last match is older than resolve_after_s.

    `now` is wall-clock time: an emitter that disappears sends no events, so event
    time alone can never advance past last_seen. Assumes NTP-synced sensors."""
    resolved = []
    for alert in s.scalars(select(Alert).where(Alert.state.in_(ACTIVE_STATES))):
        if now >= _expires_at(alert):
            _resolve(alert, _expires_at(alert), "auto")
            resolved.append(alert)
    return resolved


def acknowledge(alert: Alert, now: datetime) -> None:
    if alert.state == "RESOLVED":
        raise InvalidTransition("cannot acknowledge a resolved alert")
    if alert.state == "OPEN":
        alert.state = "ACKNOWLEDGED"
        alert.acknowledged_at = now
    # already ACKNOWLEDGED: idempotent no-op


def resolve_manually(alert: Alert, now: datetime) -> None:
    if alert.state == "RESOLVED":
        raise InvalidTransition("alert is already resolved")
    _resolve(alert, now, "manual")


def _active_alert(s: Session, rule_id: str, signal_key: str) -> Alert | None:
    return s.scalar(select(Alert).where(
        Alert.rule_id == rule_id, Alert.signal_key == signal_key,
        Alert.state.in_(ACTIVE_STATES)))


def _resolved_alert_covering(s: Session, rule_id: str, signal_key: str,
                             ts: datetime) -> Alert | None:
    """The resolved alert a late observation at `ts` belongs to, if any.

    Resolved alerts that end at or after ts are candidates. Prefer the one whose span
    contains ts; for a ts in a gap between episodes, take the next one after it."""
    candidates = (select(Alert)
                  .where(Alert.rule_id == rule_id, Alert.signal_key == signal_key,
                         Alert.state == "RESOLVED", Alert.last_seen >= ts)
                  .order_by(Alert.first_seen).limit(1))
    containing = s.scalar(candidates.where(Alert.first_seen <= ts)
                          .order_by(None).order_by(Alert.first_seen.desc()))
    return containing or s.scalar(candidates)


def _expires_at(alert: Alert) -> datetime:
    return alert.last_seen + timedelta(seconds=alert.resolve_after_s)


def _already_linked(s: Session, alert_id: str, observation_id: int) -> bool:
    return s.scalar(select(AlertObservation.id).where(
        AlertObservation.alert_id == alert_id,
        AlertObservation.observation_id == observation_id)) is not None


def _link(s: Session, alert: Alert, ev: RuleMatchEvent) -> None:
    o = ev.observation
    s.add(AlertObservation(alert_id=alert.alert_id, observation_id=o.id, sensor_id=o.sensor_id,
                           timestamp=o.timestamp, frequency_mhz=o.frequency_mhz,
                           power_dbm=o.power_dbm))
    alert.occurrences += 1
    alert.peak_power_dbm = max(alert.peak_power_dbm, o.power_dbm)
    if o.sensor_id not in alert.sensor_ids:
        # Assign a new list: SQLAlchemy does not track in-place changes to a JSON column.
        alert.sensor_ids = sorted([*alert.sensor_ids, o.sensor_id])


def _resolve(alert: Alert, at: datetime, by: Literal["auto", "manual"]) -> None:
    alert.state = "RESOLVED"
    alert.resolved_at = at
    alert.resolved_by = by
