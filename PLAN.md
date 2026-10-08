# RF Alert Manager: Implementation Plan

Home task for a DevOps & Backend Engineer role (Triarii Research). Full spec: the task PDF.
Hard limit: **4 hours**. Priority from the spec: *a simple service deployed through a working
pipeline beats a feature-rich service that only runs on a laptop.*

## How to work with me (read first)

- Work **one part at a time**. Before writing code for a part, propose the design in a few
  bullets and **wait for my approval**. Do not run ahead to the next part.
- Ask before any decision not listed below.
- Small commits with clear messages. **Never squash**: full history is a deliverable.
- After each part, add 2-3 lines to `AI_USAGE.md` notes: what the AI did, what I changed or rejected.
- I must be able to explain every design choice, so explain non-obvious code briefly.

## Agreed decisions (copy the reasoning into DECISIONS.md)

| # | Decision | Why |
|---|----------|-----|
| 1 | **Two services, medium separation**: one repo, `services/ingest` and `services/alerts`, **separate Dockerfile and image each**. A small shared package (schemas, message contract) is copied into both images. | Each service can run and be tested alone; two independent CI builds. Full microservices (separate repos/pipelines) would cost too much time. |
| 2 | **Kafka** as the broker, single broker in **KRaft mode** (no Zookeeper), JVM heap limited. Topic `rf.rule-matches`, **message key = `rule_id|signal_key`**. | Durable log, replay, partitions; standard for sensor streams and I know it. The key keeps all messages of one signal in one partition, so per-signal ordering is preserved. Redis Streams was the lighter alternative. |
| 3 | **PostgreSQL, one server, two databases**: `ingest_db` and `alerts_db`. **To confirm: one DB user per service** (recommended), each with rights only on its own DB; no service uses the `postgres` superuser. Created by an init script; passwords from env. | Range queries on time/frequency, transactions and unique constraints for alert dedup. One server because everything runs on one VM anyway; moving a DB to its own server later is only a `DATABASE_URL` change. Elasticsearch rejected: no transactions/unique constraints, near-real-time refresh breaks "is there already an open alert?", heavy JVM next to Kafka. |
| 4 | **Rule-evaluation state in memory**: plain Python `dict`, no Flink/PyFlink/Quix. | The logic is small and unit-testable with no infrastructure. Limitation (document it): state resets on restart (alert opens up to `min_duration_s` later after a deploy; no data loss since observations are in the DB) and ingest must run as **a single replica**. At scale: Flink job with `keyBy` + keyed state, or Quix Streams. |
| 5 | **FastAPI + Pydantic + SQLAlchemy 2**, Python 3.12. | Built-in validation and OpenAPI; one ORM for both services. |

## Data model

**ingest_db** (owned only by the ingest service)
- `observations`: id, sensor_id, `timestamp` (event time), `received_at` (arrival time),
  frequency_mhz, bandwidth_khz, power_dbm, lat, lon.
  Indexes: (sensor_id, timestamp), (timestamp), (frequency_mhz).
- `sensors`: sensor_id (PK), last_seen, lat, lon. `last_seen` only moves forward (out-of-order safe).
- `rules`: rule_id (PK), name, type, band_min_mhz, band_max_mhz, threshold_dbm, min_duration_s,
  `max_gap_s` (default 5), resolve_after_s, severity, enabled.

**alerts_db** (owned only by the alert service)
- `alerts`: alert_id (`alrt-xxxxxxxx`), rule_id, rule_name, signal_key, state, severity,
  frequency_mhz, sensor_ids, first_seen, last_seen, occurrences, peak_power_dbm,
  resolve_after_s (copied from the message), acknowledged_at, resolved_at, resolved_by (auto|manual).
  **Partial unique index** on (rule_id, signal_key) WHERE state IN ('OPEN','ACKNOWLEDGED'),
  so the DB enforces "one active alert per signal".
- `alert_observations`: contributing observations, **copied from the Kafka message** (no
  cross-DB access). Unique (alert_id, observation_id) makes redelivered messages idempotent.

## Message contract (ingest → Kafka → alerts)

```json
{
  "event": "rule_match",
  "rule_id": "rule-ism-high-power", "rule_name": "...", "severity": "HIGH",
  "resolve_after_s": 60, "signal_key": "2437.000", "frequency_mhz": 2437.0,
  "condition_start": "2026-10-06T10:15:30Z",
  "observation": {"id": 123, "sensor_id": "sensor-03", "timestamp": "...",
                  "frequency_mhz": 2437.01, "power_dbm": -40.2}
}
```

## Behavior to implement

**Validation**: all fields required, frequency 1-6000, bandwidth > 0, power -150..+30,
timestamp ISO-8601 **UTC only** (reject other offsets), location lat/lon in range.

**Batch policy: partial accept.** Store the valid items and report invalid ones by index.
200 = all accepted, 207 = some rejected, 422 = none accepted, 413 = batch above a max size.

**power_threshold evaluation (ingest)**
- Key = (rule_id, signal_key, sensor_id). `signal_key` = frequency rounded to
  `SIGNAL_TOLERANCE_MHZ` (default 0.1). Per sensor so a weak distant sensor cannot cancel a
  close sensor's persistence.
- State per key: `first_ts`, `last_ts`, `confirmed`. **Never store the observations themselves.**
- In band and power > threshold: start a new episode if none exists or the gap from
  `last_ts` > `max_gap_s`; otherwise widen with min/max (handles out-of-order).
- Power ≤ threshold with a timestamp newer than `last_ts`: the episode breaks (deleted).
- Once `last_ts - first_ts ≥ min_duration_s`: publish a match for this and every later
  matching observation. `condition_start = first_ts`. A short burst never publishes.
- Event time only. Watermark = newest timestamp seen. Observations older than
  watermark − `MAX_LATENESS_S` (default 300) are stored but not evaluated.
- Prune stale keys periodically so memory stays bounded.
- Updating or deleting a rule clears its state.

**Publish safety**: insert observations, flush (ids assigned), evaluate, publish to Kafka,
then commit. If publish fails: roll back and return 503, so the sensor can retry the batch
with no duplicates.

**Alert lifecycle (alerts)**: OPEN → ACKNOWLEDGED → RESOLVED
- Dedup: an active alert for (rule_id, signal_key) is updated: last_seen and first_seen
  (min/max), occurrences, peak power, sensor_ids.
- ACK does not stop updates. ACK on ACKNOWLEDGED is a no-op; ACK or resolve on RESOLVED returns 409.
- Auto-resolve: a background sweeper every `SWEEP_INTERVAL_S` resolves alerts where
  `now ≥ last_seen + resolve_after_s`, using **wall clock** (a vanished emitter produces no events,
  so event time cannot advance; assumes NTP-synced sensors). `resolved_at = last_seen + resolve_after_s`.
- A match whose timestamp is already past an active alert's expiry resolves the old alert
  and opens a new one (correct even if the sweeper has not run yet).
- A late match for an already-resolved alert is attached as evidence; it never reopens the alert.
- Kafka consumer: commit offsets **after** the DB commit (at-least-once delivery, made
  idempotent by the unique constraint).

## Endpoints

**Ingest service**: `POST/GET /api/v1/observations`, `GET /api/v1/sensors`,
`GET/POST /api/v1/rules`, `PUT/DELETE /api/v1/rules/{rule_id}` (PUT = partial update, e.g.
`{"enabled": false}`), `/health` (DB + Kafka), `/metrics`.
Rules are seeded from `config/rules.yaml` at startup: only rules missing from the DB are inserted,
so API edits survive restarts.

**Alert service**: `GET /api/v1/alerts` (filters: state, severity, sensor_id, min/max frequency,
limit/offset), `GET /api/v1/alerts/{id}` (with contributing observations),
`POST /api/v1/alerts/{id}/ack`, `POST /api/v1/alerts/{id}/resolve`, `/health`
(DB + Kafka + consumer and sweeper threads alive), `/metrics`.

**Reverse proxy (Nginx)**: `/api/v1/alerts*` goes to the alert service; other `/api/v1/*` goes to ingest.
`/metrics` and per-service health are not exposed publicly.

## Simulator (`simulator/simulate.py`, standard library only)

Options: `--url`, `--rate`, `--duration`, `--emitter-off-at`, `--scenarios`, `--watch`
(prints alert states every 10 s, useful for the screenshots).

| Scenario | Expected result |
|---|---|
| Background: sensors 01-05, weak or out of band | no alert |
| Persistent: 2437 MHz, sensor-03 at -40 dBm plus sensor-05 at -47 dBm, from t=5 s | one alert with both sensors |
| Burst: 2462 MHz for 3 s at t=20 s | no alert |
| Disappear: persistent emitter stops at `--emitter-off-at` | auto-resolve |

## Tests (pytest; CI must run them)

- **Engine unit tests:**
  - a burst does not match
  - persistence matches at exactly `min_duration`
  - a gap restarts persistence
  - dropping below threshold breaks the episode
  - a weak other sensor does not cancel
  - out-of-order observations
  - too-late observations
  - disabled and out-of-band rules
- **Lifecycle unit tests:**
  - open
  - dedup
  - separate signals
  - idempotent redelivery
  - ACK keeps updating
  - auto-resolve then reopen
  - late match does not reopen
  - invalid transitions
- **API tests:**
  - batch 200/207/422
  - query filters
  - rules CRUD and file seed
  - health and metrics
- Unit tests use SQLite and an in-memory broker double, so CI needs no Kafka or Postgres.
  Optional: one integration test with real Kafka and Postgres (compose).

## Order of work and time budget

| Part | Work | Time |
|---|---|---|
| 0 | Repo skeleton, requirements, ruff, pytest config, first commit | 10 min |
| 1a | Shared schemas and message contract; validation | 15 min |
| 1b | Rule engine and its tests | 25 min |
| 1c | Ingest service: DB, rules seeding, endpoints, Kafka producer | 30 min |
| 1d | Alert service: lifecycle, Kafka consumer, sweeper, endpoints | 35 min |
| 1e | Simulator and an end-to-end run | 10 min |
| 2 | Two Dockerfiles, compose, VM as code | 45 min |
| 3 | GitHub Actions CI/CD | 45 min |
| 4 | Docs and screenshots | 30 min |

**Part 2 (decide when we get there):**
- **Dockerfiles:** multi-stage, non-root, `HEALTHCHECK`, small base image.
- **compose:** both services, Postgres with a volume and init script, Kafka (KRaft, with a volume), Nginx.
- **Config and logs:** only Nginx's port is published; config via `.env` (`.env.example` in the repo, real `.env` never committed); json-file log driver with max-size/max-file.
- **VM:** Ubuntu 24.04 defined as code (**open: local Vagrant or cloud with Terraform**), cloud-init or a provision script, a systemd unit so compose starts on boot, ufw firewall.

**Part 3 (decide when we get there):** GitHub Actions.
- **On PR or push:** ruff and pytest; a failing check blocks the merge (branch protection).
- **On merge to main:** build both images, tag with the commit SHA, push to GHCR.
- **Deploy:** that exact SHA to the VM (if the VM is local: a self-hosted runner on the VM, no inbound access needed), then a smoke test through Nginx that fails the pipeline if unhealthy.
- **Rollback:** a `workflow_dispatch` with the previous SHA, documented and demonstrable.

## Deliverables checklist

- [ ] Git repo with full history (GitHub)
- [ ] README.md: provision the VM, deploy, run locally, run tests, run the simulator
- [ ] ARCHITECTURE.md: components, data flow, data model, deployment topology, diagram
- [ ] DECISIONS.md: decisions above, batch policy, late-data handling, not production-ready, future work
- [ ] AI_USAGE.md: tools used, for what, where the AI was wrong, what I changed
- [ ] Screenshots: VM, `docker ps`, simulator opening and resolving alerts, commit going through the pipeline

## "Not production-ready / future work" (for DECISIONS.md)

- In-memory rule state limits ingest to one replica. At scale: Flink keyed state or Quix Streams.
- One Postgres server: split physically, observations first (noisy neighbor, different retention).
- Observations at scale: TimescaleDB, or Kafka Connect to Elasticsearch for analytics.
- No migrations tool (`create_all`); production would use Alembic.
- Single Kafka broker, replication factor 1.
- Auto-resolve uses wall clock and assumes synced sensor clocks.
- Optional rule types (`multi_sensor`, `sensor_silence`), webhooks, Grafana: only if time is left.

## Open questions to ask me before the relevant part

1. Confirm one DB user per service (recommended) or a single user.
2. Kafka Python client: `confluent-kafka` (librdkafka, recommended) or `aiokafka`.
3. VM: local Vagrant (with a self-hosted runner) or cloud free tier (with Terraform).

## Note about existing code

An earlier prototype exists in this session's bundle (`rf-alert-manager.bundle`). It was built
**before** these decisions, with Redis Streams, one shared image and one shared DB. Its rule
engine, alert lifecycle, simulator and tests match the behavior above and can be reused. The
broker layer, packaging and DB setup must change to Kafka, two images and two DBs. Ask me
whether to start from it or from scratch.