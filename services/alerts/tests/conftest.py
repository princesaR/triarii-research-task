from datetime import UTC, datetime, timedelta
from itertools import count

import pytest
from fastapi.testclient import TestClient

from alerts.main import create_app
from alerts.settings import AlertSettings
from db.alerts import Base
from models import MatchedObservation, RuleMatchEvent
from rfam_common.db import make_engine, make_session_factory

T0 = datetime(2026, 10, 6, 10, 0, 0, tzinfo=UTC)
_obs_ids = count(1)


def make_match(seconds: float, sensor="sensor-03", signal="2437.000", power=-40.0,
               rule_id="rule-ism-high-power", start: float | None = None,
               obs_id: int | None = None, resolve_after_s=60) -> RuleMatchEvent:
    """A rule match as ingest would publish it, for an observation `seconds` after T0."""
    ts = T0 + timedelta(seconds=seconds)
    return RuleMatchEvent(
        rule_id=rule_id, rule_name="High power in protected ISM band", severity="HIGH",
        resolve_after_s=resolve_after_s, signal_key=signal, frequency_mhz=float(signal),
        condition_start=T0 + timedelta(seconds=seconds if start is None else start),
        observation=MatchedObservation(
            id=next(_obs_ids) if obs_id is None else obs_id, sensor_id=sensor, timestamp=ts,
            frequency_mhz=float(signal) + 0.01, power_dbm=power))


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


@pytest.fixture
def match():
    return make_match


@pytest.fixture
def session():
    engine = make_engine("sqlite://")
    Base.metadata.create_all(engine)
    with make_session_factory(engine)() as s:
        yield s


@pytest.fixture
def app():
    return create_app(AlertSettings(database_url="sqlite://"), run_workers=False)


@pytest.fixture
def ctx(app):
    return app.state.ctx


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c
