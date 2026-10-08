from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from ingest.main import create_app
from ingest.settings import DEFAULT_RULES_FILE, IngestSettings
from rfam_common.kafka import InMemoryPublisher

T0 = datetime(2026, 10, 6, 10, 0, 0, tzinfo=UTC)


def make_obs(seconds: float, sensor="sensor-03", freq=2437.0, power=-40.0) -> dict:
    """One valid observation as JSON, `seconds` after T0."""
    return {
        "sensor_id": sensor,
        "timestamp": (T0 + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "frequency_mhz": freq,
        "bandwidth_khz": 20000,
        "power_dbm": power,
        "location": {"lat": 32.08, "lon": 34.78},
    }


@pytest.fixture
def obs():
    return make_obs


@pytest.fixture
def publisher():
    return InMemoryPublisher()


@pytest.fixture
def make_client(publisher):
    """Build a client with custom settings; SQLite in memory and the default rules file."""
    def _make(**overrides) -> TestClient:
        settings = IngestSettings(**({"database_url": "sqlite://",
                                      "rules_file": DEFAULT_RULES_FILE} | overrides))
        return TestClient(create_app(settings, publisher=publisher))
    return _make


@pytest.fixture
def ingest(make_client):
    with make_client() as client:
        yield client
