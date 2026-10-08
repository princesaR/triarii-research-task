"""Shared state of the alert service, built once in create_app() and handed out via Depends."""

import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from alerts import lifecycle
from alerts.metrics import Metrics
from alerts.settings import AlertSettings
from db.alerts import ACTIVE_STATES, Alert
from models import RuleMatchEvent
from rfam_common.db import utcnow

log = logging.getLogger("alerts")


@dataclass
class AlertsContext:
    settings: AlertSettings
    db_engine: Engine
    session_factory: sessionmaker[Session]
    metrics: Metrics
    # The Kafka consumer, the sweeper and the ack/resolve API all change alerts:
    # one lock serializes them, so they never race on the same row.
    lock: threading.Lock = field(default_factory=threading.Lock)
    consumer: object | None = None  # KafkaConsumerLoop (None in unit tests)
    threads: list[threading.Thread] = field(default_factory=list)

    def handle_match(self, ev: RuleMatchEvent) -> lifecycle.Action:
        """Kafka handler: fold one match into the DB and commit. Raises on DB errors,
        so the consumer retries without committing the Kafka offset."""
        with self.session_factory() as s, self.lock:
            alert, action = lifecycle.apply_match(s, ev)
            s.commit()
        self.metrics.matches.labels(action).inc()
        if action == "opened":
            self.metrics.opened.labels(alert.rule_id, alert.severity).inc()
            log.info("alert %s OPENED rule=%s freq=%.3f sensor=%s", alert.alert_id,
                     alert.rule_id, alert.frequency_mhz, ev.observation.sensor_id)
        return action

    def sweep_once(self) -> int:
        """Auto-resolve expired alerts (wall clock) and refresh the active-alerts gauge."""
        with self.session_factory() as s, self.lock:
            resolved = lifecycle.sweep_expired(s, utcnow())
            s.commit()
            active = s.scalar(select(func.count()).select_from(Alert)
                              .where(Alert.state.in_(ACTIVE_STATES)))
        for a in resolved:
            self.metrics.resolved.labels("auto").inc()
            log.info("alert %s AUTO-RESOLVED at %s", a.alert_id, a.resolved_at)
        self.metrics.active.set(active or 0)
        return len(resolved)


def get_ctx(request: Request) -> AlertsContext:
    return request.app.state.ctx


def get_session(request: Request) -> Iterator[Session]:
    with request.app.state.ctx.session_factory() as session:
        yield session


CtxDep = Annotated[AlertsContext, Depends(get_ctx)]
SessionDep = Annotated[Session, Depends(get_session)]
