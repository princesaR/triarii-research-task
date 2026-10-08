def test_health_ok(ingest):
    r = ingest.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "checks": {"database": True, "kafka": True}}


def test_health_degraded_when_kafka_down(ingest, publisher):
    publisher.fail = True
    r = ingest.get("/health")
    assert r.status_code == 503 and r.json()["checks"]["kafka"] is False


def test_metrics_count_observations(ingest, obs):
    ingest.post("/api/v1/observations", json=[obs(0), obs(1, power=99)])
    text = ingest.get("/metrics").text
    assert 'rf_observations_total{result="accepted"} 1.0' in text
    assert 'rf_observations_total{result="rejected"} 1.0' in text
