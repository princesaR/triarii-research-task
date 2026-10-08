from datetime import UTC, datetime, timedelta


def test_list_and_detail(client, ctx, match):
    ctx.handle_match(match(10, start=0))
    ctx.handle_match(match(11, sensor="sensor-05", power=-38.2))

    [alert] = client.get("/api/v1/alerts").json()
    # The spec's Alert Model fields
    assert set(alert) >= {"alert_id", "rule_id", "state", "severity", "frequency_mhz",
                          "sensor_ids", "first_seen", "last_seen", "occurrences",
                          "peak_power_dbm"}
    assert alert["state"] == "OPEN" and alert["occurrences"] == 2
    assert alert["sensor_ids"] == ["sensor-03", "sensor-05"]
    assert alert["peak_power_dbm"] == -38.2
    assert alert["first_seen"].startswith("2026-10-06T10:00:00")

    detail = client.get(f"/api/v1/alerts/{alert['alert_id']}").json()
    assert [o["sensor_id"] for o in detail["observations"]] == ["sensor-03", "sensor-05"]
    assert client.get("/api/v1/alerts/alrt-nope").status_code == 404


def test_filters(client, ctx, match):
    ctx.handle_match(match(10, signal="2437.000"))
    ctx.handle_match(match(10, signal="5800.000", sensor="sensor-07",
                           rule_id="rule-5g8-high-power"))
    get = lambda **p: [a["frequency_mhz"] for a in client.get("/api/v1/alerts", params=p).json()]  # noqa: E731
    assert get(min_freq_mhz=5000) == [5800.0]
    assert get(max_freq_mhz=3000) == [2437.0]
    assert get(sensor_id="sensor-07") == [5800.0]
    assert get(state="RESOLVED") == []
    assert sorted(get(state="OPEN")) == [2437.0, 5800.0]
    assert get(severity="LOW") == []
    assert len(get(limit=1)) == 1
    assert client.get("/api/v1/alerts", params={"state": "BAD"}).status_code == 422


def test_ack_and_resolve_endpoints(client, ctx, match):
    ctx.handle_match(match(10))
    alert_id = client.get("/api/v1/alerts").json()[0]["alert_id"]

    r = client.post(f"/api/v1/alerts/{alert_id}/ack")
    assert r.status_code == 200 and r.json()["state"] == "ACKNOWLEDGED"
    assert client.post(f"/api/v1/alerts/{alert_id}/ack").status_code == 200  # no-op

    ctx.handle_match(match(12))  # ack does not stop updates
    assert client.get(f"/api/v1/alerts/{alert_id}").json()["occurrences"] == 2

    r = client.post(f"/api/v1/alerts/{alert_id}/resolve")
    assert r.json()["state"] == "RESOLVED" and r.json()["resolved_by"] == "manual"
    assert client.post(f"/api/v1/alerts/{alert_id}/resolve").status_code == 409
    assert client.post(f"/api/v1/alerts/{alert_id}/ack").status_code == 409
    assert client.post("/api/v1/alerts/alrt-nope/ack").status_code == 404


def test_sweep_auto_resolves_with_wall_clock(client, ctx, match):
    # A match whose observation happened 2 minutes ago: resolve_after_s=60 has passed.
    old = match(0, resolve_after_s=60)
    old.observation.timestamp = datetime.now(UTC) - timedelta(minutes=2)
    ctx.handle_match(old)
    assert ctx.sweep_once() == 1
    [alert] = client.get("/api/v1/alerts").json()
    assert alert["state"] == "RESOLVED" and alert["resolved_by"] == "auto"
    assert 'rf_alerts_resolved_total{by="auto"} 1.0' in client.get("/metrics").text


def test_health_and_metrics(client, ctx, match):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["checks"] == {"database": True}
    ctx.handle_match(match(10))
    ctx.handle_match(match(11))
    text = client.get("/metrics").text
    assert 'rf_alert_matches_total{action="opened"} 1.0' in text
    assert 'rf_alert_matches_total{action="updated"} 1.0' in text
