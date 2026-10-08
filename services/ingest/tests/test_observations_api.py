def test_single_observation_is_accepted(ingest, obs):
    r = ingest.post("/api/v1/observations", json=obs(0))
    assert r.status_code == 200
    assert r.json() == {"accepted": 1, "rejected": []}


def test_batch_partial_accept_reports_invalid_items(ingest, obs):
    bad_freq = obs(1, freq=7000)
    not_utc = obs(2) | {"timestamp": "2026-10-06T12:00:00+03:00"}
    missing = obs(3)
    del missing["power_dbm"]
    r = ingest.post("/api/v1/observations", json=[obs(0), bad_freq, not_utc, missing])
    assert r.status_code == 207
    body = r.json()
    assert body["accepted"] == 1
    assert [x["index"] for x in body["rejected"]] == [1, 2, 3]
    assert body["rejected"][0]["errors"][0]["field"] == "frequency_mhz"


def test_all_invalid_returns_422(ingest, obs):
    r = ingest.post("/api/v1/observations", json=[obs(0, power=99)])
    assert r.status_code == 422
    assert r.json()["accepted"] == 0


def test_empty_and_oversized_batches(make_client, obs):
    with make_client(max_batch_size=2) as client:
        assert client.post("/api/v1/observations", json=[]).status_code == 422
        too_big = [obs(s) for s in range(3)]
        assert client.post("/api/v1/observations", json=too_big).status_code == 413


def test_persistent_emitter_publishes_burst_does_not(ingest, obs, publisher):
    burst = [obs(s, sensor="sensor-04", freq=2462.0) for s in range(0, 3)]
    ingest.post("/api/v1/observations", json=burst)
    assert publisher.messages == []

    ingest.post("/api/v1/observations", json=[obs(s) for s in range(0, 15)])
    assert len(publisher.messages) == 5  # seconds 10..14
    msg = publisher.messages[0]
    assert msg["rule_id"] == "rule-ism-high-power"
    assert msg["signal_key"] == "2437.000"
    assert msg["condition_start"].startswith("2026-10-06T10:00:00")
    assert publisher.keys[0] == "rule-ism-high-power|2437.000"


def test_publish_failure_rolls_back_and_returns_503(ingest, obs, publisher):
    publisher.fail = True
    r = ingest.post("/api/v1/observations", json=[obs(s) for s in range(0, 15)])
    assert r.status_code == 503
    assert ingest.get("/api/v1/observations").json() == []  # nothing stored
    assert ingest.get("/api/v1/sensors").json() == []

    publisher.fail = False  # the sensor retries the same batch
    assert ingest.post("/api/v1/observations",
                       json=[obs(s) for s in range(15, 30)]).status_code == 200


def test_batch_without_matches_does_not_need_kafka(ingest, obs, publisher):
    publisher.fail = True
    r = ingest.post("/api/v1/observations", json=obs(0, power=-90))
    assert r.status_code == 200  # nothing to publish, so nothing can fail


def test_query_filters(ingest, obs):
    ingest.post("/api/v1/observations",
                json=[obs(0), obs(5, sensor="sensor-07", freq=900.0), obs(10)])
    r = ingest.get("/api/v1/observations", params={"min_freq_mhz": 2400, "max_freq_mhz": 2500})
    assert [o["sensor_id"] for o in r.json()] == ["sensor-03", "sensor-03"]
    assert len(ingest.get("/api/v1/observations", params={"sensor_id": "sensor-07"}).json()) == 1
    r = ingest.get("/api/v1/observations", params={"start": "2026-10-06T10:00:05Z",
                                                   "end": "2026-10-06T10:00:10Z"})
    assert [o["sensor_id"] for o in r.json()] == ["sensor-07"]
    assert len(ingest.get("/api/v1/observations", params={"limit": 1}).json()) == 1


def test_sensor_last_seen_never_moves_back(ingest, obs):
    ingest.post("/api/v1/observations", json=obs(10))
    ingest.post("/api/v1/observations", json=obs(0))  # late arrival
    [sensor] = ingest.get("/api/v1/sensors").json()
    assert sensor["last_seen"].startswith("2026-10-06T10:00:10")
