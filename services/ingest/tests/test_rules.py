import json

import pytest
from pydantic import ValidationError

from ingest.rules import read_rules_file
from ingest.settings import DEFAULT_RULES_FILE

NEW_RULE = {"rule_id": "rule-test", "name": "t", "type": "power_threshold",
            "band": {"min_mhz": 100, "max_mhz": 200}, "threshold_dbm": -60,
            "min_duration_s": 2, "resolve_after_s": 30, "severity": "LOW"}


def test_default_yaml_file_is_seeded(ingest):
    ids = {r["rule_id"] for r in ingest.get("/api/v1/rules").json()}
    assert ids == {"rule-ism-high-power", "rule-5g8-high-power"}


def test_json_rules_file_bare_list_and_wrapped(tmp_path):
    bare = tmp_path / "rules.json"
    bare.write_text(json.dumps([NEW_RULE]))
    wrapped = tmp_path / "wrapped.json"
    wrapped.write_text(json.dumps({"rules": [NEW_RULE]}))
    assert [r.rule_id for r in read_rules_file(str(bare))] == ["rule-test"]
    assert [r.rule_id for r in read_rules_file(str(wrapped))] == ["rule-test"]


def test_invalid_rules_file_fails_fast(tmp_path):
    bad = tmp_path / "rules.yaml"
    bad.write_text("rules:\n  - rule_id: x\n    name: x\n")
    with pytest.raises(ValidationError):
        read_rules_file(str(bad))
    dup = tmp_path / "dup.json"
    dup.write_text(json.dumps([NEW_RULE, NEW_RULE]))
    with pytest.raises(ValueError, match="duplicate"):
        read_rules_file(str(dup))


def test_missing_rules_file_starts_empty(make_client, tmp_path):
    with make_client(rules_file=str(tmp_path / "nope.yaml")) as client:
        assert client.get("/api/v1/rules").json() == []


def test_api_edits_survive_restart(make_client, tmp_path):
    db = f"sqlite:///{tmp_path / 'ingest.db'}"
    with make_client(database_url=db) as client:
        client.put("/api/v1/rules/rule-ism-high-power", json={"threshold_dbm": -30})
    with make_client(database_url=db) as client:  # restart: file is seeded again
        rule = client.get("/api/v1/rules/rule-ism-high-power").json()
        assert rule["threshold_dbm"] == -30  # API edit kept, file value not re-applied


def test_crud(ingest):
    assert ingest.post("/api/v1/rules", json=NEW_RULE).status_code == 201
    assert ingest.post("/api/v1/rules", json=NEW_RULE).status_code == 409
    assert ingest.get("/api/v1/rules/rule-test").json()["max_gap_s"] == 5  # default

    r = ingest.put("/api/v1/rules/rule-test", json={"enabled": False})
    assert r.status_code == 200 and r.json()["enabled"] is False
    assert r.json()["threshold_dbm"] == -60  # untouched

    r = ingest.put("/api/v1/rules/rule-test", json={"band": {"min_mhz": 150, "max_mhz": 250}})
    assert r.json()["band"] == {"min_mhz": 150, "max_mhz": 250}

    assert ingest.delete("/api/v1/rules/rule-test").status_code == 204
    assert ingest.delete("/api/v1/rules/rule-test").status_code == 404
    assert ingest.get("/api/v1/rules/rule-test").status_code == 404
    assert ingest.put("/api/v1/rules/rule-test", json={"enabled": True}).status_code == 404


def test_invalid_rule_rejected(ingest):
    bad_band = NEW_RULE | {"band": {"min_mhz": 300, "max_mhz": 200}}
    assert ingest.post("/api/v1/rules", json=bad_band).status_code == 422
    assert ingest.post("/api/v1/rules", json=NEW_RULE | {"severity": "X"}).status_code == 422
    assert ingest.put("/api/v1/rules/rule-ism-high-power",
                      json={"threshold_dbm": None}).status_code == 422


def test_disabled_rule_stops_matching(ingest, publisher, obs):
    ingest.put("/api/v1/rules/rule-ism-high-power", json={"enabled": False})
    ingest.post("/api/v1/observations", json=[obs(s) for s in range(0, 15)])
    assert publisher.messages == []


def test_rule_update_restarts_persistence(ingest, publisher, obs):
    ingest.post("/api/v1/observations", json=[obs(s) for s in range(0, 8)])
    ingest.put("/api/v1/rules/rule-ism-high-power", json={"threshold_dbm": -45})
    ingest.post("/api/v1/observations", json=[obs(s) for s in range(8, 12)])
    assert publisher.messages == []  # 8 s of old-rule persistence did not carry over


def test_new_rule_via_api_is_used_immediately(ingest, publisher, obs):
    ingest.post("/api/v1/rules", json=NEW_RULE)  # min_duration_s = 2
    ingest.post("/api/v1/observations", json=[obs(s, freq=150.0, power=-50) for s in range(3)])
    assert [m["rule_id"] for m in publisher.messages] == ["rule-test"]


def test_default_rules_file_exists():
    assert read_rules_file(DEFAULT_RULES_FILE)
