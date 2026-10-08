from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ingest.engine import RuleEngine, signal_key

T0 = datetime(2026, 10, 6, 10, 0, 0, tzinfo=UTC)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


@dataclass
class Rule:
    rule_id: str = "rule-ism-high-power"
    name: str = "High power in protected ISM band"
    type: str = "power_threshold"
    band_min_mhz: float = 2400.0
    band_max_mhz: float = 2483.5
    threshold_dbm: float = -50.0
    min_duration_s: float = 10
    max_gap_s: float = 5
    resolve_after_s: float = 60
    severity: str = "HIGH"
    enabled: bool = True


@dataclass
class Obs:
    timestamp: datetime
    sensor_id: str = "sensor-03"
    frequency_mhz: float = 2437.0
    power_dbm: float = -40.0


def run(engine, rule, observations):
    return [engine.evaluate(o, [rule]) for o in observations]


def test_short_burst_does_not_match():
    engine, rule = RuleEngine(), Rule()
    results = run(engine, rule, [Obs(at(s)) for s in range(0, 4)])  # 3 s burst
    assert all(r == [] for r in results)


def test_persistent_signal_matches_at_exactly_min_duration():
    engine, rule = RuleEngine(), Rule()
    results = run(engine, rule, [Obs(at(s)) for s in range(0, 15)])
    assert all(r == [] for r in results[:10])  # 0..9 s: still pending
    assert results[10] and results[10][0].condition_start == at(0)  # 10 s reached
    assert all(r for r in results[10:])


def test_gap_longer_than_max_gap_restarts_persistence():
    engine, rule = RuleEngine(), Rule()
    run(engine, rule, [Obs(at(s)) for s in range(0, 6)])               # 0-5 s
    results = run(engine, rule, [Obs(at(s)) for s in range(20, 26)])  # 20-25 s
    assert all(r == [] for r in results)  # two bursts must not add up


def test_drop_below_threshold_breaks_episode():
    engine, rule = RuleEngine(), Rule()
    run(engine, rule, [Obs(at(s)) for s in range(0, 8)])
    engine.evaluate(Obs(at(8), power_dbm=-80), [rule])
    assert engine.evaluate(Obs(at(11)), [rule]) == []


def test_weak_reading_from_other_sensor_does_not_cancel():
    engine, rule = RuleEngine(), Rule()
    for s in range(0, 11):
        engine.evaluate(Obs(at(s), sensor_id="sensor-09", power_dbm=-95), [rule])
        result = engine.evaluate(Obs(at(s)), [rule])
    assert result  # sensor-03 kept its persistence


def test_out_of_order_observations_use_event_time():
    engine, rule = RuleEngine(), Rule()
    order = [0, 2, 1, 4, 3, 6, 5, 8, 7, 10, 9]
    results = run(engine, rule, [Obs(at(s)) for s in order])
    assert results[9] and results[9][0].condition_start == at(0)


def test_too_late_observation_is_ignored():
    engine, rule = RuleEngine(max_lateness_s=60), Rule(min_duration_s=0)
    engine.evaluate(Obs(at(1000)), [rule])
    assert engine.evaluate(Obs(at(0)), [rule]) == []


def test_out_of_band_and_disabled_rules_are_ignored():
    engine = RuleEngine()
    assert engine.evaluate(Obs(at(0), frequency_mhz=5800), [Rule(min_duration_s=0)]) == []
    assert engine.evaluate(Obs(at(0)), [Rule(min_duration_s=0, enabled=False)]) == []


def test_clear_rule_drops_its_state():
    engine, rule = RuleEngine(), Rule()
    run(engine, rule, [Obs(at(s)) for s in range(0, 8)])
    engine.clear_rule(rule.rule_id)
    assert engine.episode_count == 0
    assert engine.evaluate(Obs(at(11)), [rule]) == []  # persistence starts over


def test_prune_drops_stale_episodes():
    engine, rule = RuleEngine(max_lateness_s=60), Rule()
    engine.evaluate(Obs(at(0)), [rule])
    engine.evaluate(Obs(at(1000), frequency_mhz=900), [rule])  # only advances the watermark
    engine.prune()
    assert engine.episode_count == 0


def test_signal_key_buckets_nearby_frequencies():
    assert signal_key(2437.01, 0.1) == signal_key(2436.98, 0.1) == "2437.000"
    assert signal_key(2437.0, 0.1) != signal_key(2437.3, 0.1)
