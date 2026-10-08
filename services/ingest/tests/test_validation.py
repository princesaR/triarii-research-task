import pytest
from pydantic import ValidationError

from models import Band, ObservationIn, RuleUpdate


def test_valid_observation(obs):
    assert ObservationIn.model_validate(obs(0)).sensor_id == "sensor-03"


@pytest.mark.parametrize("field,value", [
    ("frequency_mhz", 0.5), ("frequency_mhz", 6000.1),
    ("bandwidth_khz", 0),
    ("power_dbm", -150.1), ("power_dbm", 30.1),
    ("location", {"lat": 91, "lon": 0}), ("location", {"lat": 0, "lon": 181}),
])
def test_out_of_range_values_rejected(field, value, obs):
    with pytest.raises(ValidationError):
        ObservationIn.model_validate(obs(0) | {field: value})


@pytest.mark.parametrize("field,value", [
    ("frequency_mhz", 1), ("frequency_mhz", 6000), ("power_dbm", -150), ("power_dbm", 30),
])
def test_boundary_values_accepted(field, value, obs):
    ObservationIn.model_validate(obs(0) | {field: value})


@pytest.mark.parametrize("ts", ["2026-10-06T12:00:00+03:00", "2026-10-06T10:00:00"])
def test_timestamp_must_be_utc(ts, obs):
    with pytest.raises(ValidationError, match="UTC"):
        ObservationIn.model_validate(obs(0) | {"timestamp": ts})


def test_utc_offset_zero_accepted(obs):
    ObservationIn.model_validate(obs(0) | {"timestamp": "2026-10-06T10:00:00+00:00"})


def test_missing_and_unknown_fields_rejected(obs):
    missing = obs(0)
    del missing["power_dbm"]
    with pytest.raises(ValidationError):
        ObservationIn.model_validate(missing)
    with pytest.raises(ValidationError):
        ObservationIn.model_validate(obs(0) | {"extra": 1})


def test_band_must_be_ordered():
    with pytest.raises(ValidationError, match="lower"):
        Band(min_mhz=300, max_mhz=200)


def test_rule_update_rejects_explicit_null():
    with pytest.raises(ValidationError, match="null"):
        RuleUpdate.model_validate({"threshold_dbm": None})
    assert RuleUpdate.model_validate({"enabled": False}).model_fields_set == {"enabled"}


def test_out_models_build_from_db_rows():
    from datetime import UTC, datetime

    from ingest.models import Observation, Rule, Sensor
    from models import ObservationOut, RuleOut, SensorOut

    ts = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)
    o = ObservationOut.model_validate(Observation(
        id=1, sensor_id="s1", timestamp=ts, received_at=ts, frequency_mhz=2437.0,
        bandwidth_khz=20, power_dbm=-40, lat=32.0, lon=34.0))
    assert o.location.lat == 32.0
    sensor = SensorOut.model_validate(Sensor(sensor_id="s1", last_seen=ts, lat=1, lon=2))
    assert sensor.location.lon == 2
    r = RuleOut.model_validate(Rule(
        rule_id="r", name="n", type="power_threshold", band_min_mhz=100, band_max_mhz=200,
        threshold_dbm=-50, min_duration_s=10, max_gap_s=5, resolve_after_s=60,
        severity="HIGH", enabled=True))
    assert r.band.max_mhz == 200


@pytest.mark.parametrize("ts", [1791281730, "1791281730", "2026-10-06", "06/10/2026 10:15"])
def test_timestamp_must_be_iso_8601_string(ts, obs):
    with pytest.raises(ValidationError, match="ISO-8601"):
        ObservationIn.model_validate(obs(0) | {"timestamp": ts})


@pytest.mark.parametrize("field,value", [
    ("frequency_mhz", "2437"), ("power_dbm", True), ("bandwidth_khz", float("inf")),
    ("power_dbm", float("nan")), ("sensor_id", 3),
    ("location", {"lat": "32.08", "lon": 34.78}),
])
def test_wrong_types_rejected(field, value, obs):
    with pytest.raises(ValidationError):
        ObservationIn.model_validate(obs(0) | {field: value})


def test_spec_example_accepted():
    ObservationIn.model_validate_json("""{
        "sensor_id": "sensor-03", "timestamp": "2026-10-06T10:15:30Z",
        "frequency_mhz": 2437.0, "bandwidth_khz": 20000, "power_dbm": -42.5,
        "location": {"lat": 32.08, "lon": 34.78}}""")


SPEC_RULE = {
    "rule_id": "rule-ism-high-power", "name": "High power in protected ISM band",
    "type": "power_threshold", "band": {"min_mhz": 2400.0, "max_mhz": 2483.5},
    "threshold_dbm": -50.0, "min_duration_s": 10, "resolve_after_s": 60,
    "severity": "HIGH", "enabled": True,
}


def test_spec_rule_example_accepted():
    from models import RuleIn

    rule = RuleIn.model_validate(SPEC_RULE)
    assert rule.max_gap_s == 5  # our optional extension, defaulted


@pytest.mark.parametrize("missing", ["type", "min_duration_s", "band", "threshold_dbm",
                                     "resolve_after_s", "severity", "name", "rule_id"])
def test_rule_required_fields(missing):
    from models import RuleIn

    with pytest.raises(ValidationError):
        RuleIn.model_validate({k: v for k, v in SPEC_RULE.items() if k != missing})


@pytest.mark.parametrize("field,value", [
    ("min_duration_s", 0),            # a single reading would open an alert
    ("enabled", "yes"), ("enabled", 1),
    ("threshold_dbm", "-50"), ("threshold_dbm", -151), ("threshold_dbm", 31),
    ("resolve_after_s", True), ("resolve_after_s", 0),
    ("severity", "high"), ("type", "multi_sensor"), ("type", "unknown"),
    ("rule_id", "has space"), ("name", 5), ("max_gap_s", 0),
    ("band", {"min_mhz": 2400, "max_mhz": float("inf")}),
    ("band", {"min_mhz": "2400", "max_mhz": 2483.5}),
])
def test_rule_invalid_values_rejected(field, value):
    from models import RuleIn

    with pytest.raises(ValidationError):
        RuleIn.model_validate(SPEC_RULE | {field: value})


@pytest.mark.parametrize("patch", [{"enabled": "yes"}, {"type": "power_threshold"},
                                   {"rule_id": "x"}, {"min_duration_s": 0}])
def test_rule_update_invalid(patch):
    with pytest.raises(ValidationError):
        RuleUpdate.model_validate(patch)
