from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from alerts import lifecycle
from db.alerts import Alert, AlertObservation

T0 = datetime(2026, 10, 6, 10, 0, 0, tzinfo=UTC)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def apply(s, ev):
    alert, action = lifecycle.apply_match(s, ev)
    s.commit()
    return alert, action


def n_alerts(s) -> int:
    return s.scalar(select(func.count()).select_from(Alert))


def test_first_match_opens_alert(session, match):
    alert, action = apply(session, match(10, start=0))
    assert action == "opened"
    assert alert.alert_id.startswith("alrt-") and alert.state == "OPEN"
    assert alert.first_seen == at(0)  # condition_start: when persistence began
    assert alert.last_seen == at(10)
    assert alert.occurrences == 1 and alert.sensor_ids == ["sensor-03"]
    assert alert.frequency_mhz == 2437.0 and alert.severity == "HIGH"


def test_same_signal_deduplicates_and_updates(session, match):
    a1, _ = apply(session, match(10, start=0, power=-40))
    a2, action = apply(session, match(11, sensor="sensor-05", power=-38.2))
    assert action == "updated" and a2.alert_id == a1.alert_id
    assert a2.occurrences == 2 and a2.last_seen == at(11)
    assert a2.peak_power_dbm == -38.2
    assert a2.sensor_ids == ["sensor-03", "sensor-05"]
    assert n_alerts(session) == 1


def test_different_signal_or_rule_opens_separate_alerts(session, match):
    apply(session, match(10))
    apply(session, match(10, signal="2462.000"))
    apply(session, match(10, rule_id="rule-other"))
    assert n_alerts(session) == 3


def test_redelivered_message_is_counted_once(session, match):
    ev = match(10)
    apply(session, ev)
    alert, action = apply(session, ev)  # Kafka at-least-once: same message again
    assert action == "duplicate" and alert.occurrences == 1
    assert session.scalar(select(func.count()).select_from(AlertObservation)) == 1


def test_out_of_order_match_widens_by_event_time(session, match):
    apply(session, match(20, start=15))
    alert, action = apply(session, match(12, start=12))  # arrives late, happened earlier
    assert action == "updated"
    assert alert.first_seen == at(12) and alert.last_seen == at(20)


def test_acknowledged_alert_keeps_updating(session, match):
    alert, _ = apply(session, match(10))
    lifecycle.acknowledge(alert, at(11))
    session.commit()
    alert, action = apply(session, match(12))
    assert action == "updated"
    assert alert.state == "ACKNOWLEDGED" and alert.occurrences == 2


def test_ack_twice_is_noop_and_ack_or_resolve_on_resolved_fails(session, match):
    alert, _ = apply(session, match(10))
    lifecycle.acknowledge(alert, at(11))
    lifecycle.acknowledge(alert, at(50))
    assert alert.acknowledged_at == at(11)
    lifecycle.resolve_manually(alert, at(60))
    assert alert.state == "RESOLVED" and alert.resolved_by == "manual"
    with pytest.raises(lifecycle.InvalidTransition):
        lifecycle.acknowledge(alert, at(61))
    with pytest.raises(lifecycle.InvalidTransition):
        lifecycle.resolve_manually(alert, at(61))


def test_sweeper_auto_resolves_after_resolve_after_s(session, match):
    alert, _ = apply(session, match(10, resolve_after_s=60))
    assert lifecycle.sweep_expired(session, now=at(69)) == []  # 59 s of silence: not yet
    assert lifecycle.sweep_expired(session, now=at(70)) == [alert]
    assert alert.state == "RESOLVED" and alert.resolved_by == "auto"
    assert alert.resolved_at == at(70)  # the expiry moment, not when the sweeper ran


def test_new_match_after_resolution_opens_new_alert(session, match):
    old, _ = apply(session, match(10))
    lifecycle.sweep_expired(session, now=at(70))
    session.commit()
    new, action = apply(session, match(100))
    assert action == "opened" and new.alert_id != old.alert_id
    assert old.state == "RESOLVED" and new.state == "OPEN"


def test_match_past_expiry_resolves_old_alert_even_before_sweep(session, match):
    old, _ = apply(session, match(10, resolve_after_s=60))
    new, action = apply(session, match(100, resolve_after_s=60))  # sweeper never ran
    assert action == "opened" and new.alert_id != old.alert_id
    assert old.state == "RESOLVED" and old.resolved_at == at(70)


def test_late_match_does_not_reopen_resolved_alert(session, match):
    alert, _ = apply(session, match(10))
    apply(session, match(20))
    lifecycle.sweep_expired(session, now=at(80))
    session.commit()
    late, action = apply(session, match(15))  # happened during the alert, arrived after
    assert action == "late" and late.alert_id == alert.alert_id
    assert late.state == "RESOLVED" and late.occurrences == 3
    assert n_alerts(session) == 1


def test_very_late_match_goes_to_its_own_episode_not_the_active_one(session, match):
    old, _ = apply(session, match(10))
    apply(session, match(20))
    lifecycle.sweep_expired(session, now=at(80))
    session.commit()
    new, _ = apply(session, match(200, start=190))  # a new episode is active now
    late, action = apply(session, match(12))        # very late, from the first episode
    assert action == "late" and late.alert_id == old.alert_id
    assert new.first_seen == at(190)  # the active alert was not stretched back in time
