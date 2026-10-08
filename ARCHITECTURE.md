# Architecture

RF Alert Manager receives RF observations from sensors, evaluates detection rules on them and
manages the resulting alerts (open → acknowledge → resolve).

## Components

```
                          ┌──────────────────────── VM (Ubuntu 24.04) ─────────────────────────┐
                          │  docker compose (systemd unit starts it on boot), ufw: 22, 80 only │
 sensors / simulator      │                                                                    │
 ───── HTTP :80 ─────────►│  Nginx ──/api/v1/alerts*──►  alerts service ◄──┐                   │
                          │    │                         (FastAPI)         │ consume           │
                          │    └──other /api/v1/*──►  ingest service       │ rf.rule-matches   │
                          │                          (FastAPI + rule       │                   │
                          │                           engine) ──produce──► Kafka (KRaft, 1 node)│
                          │                              │                     │               │
                          │                              ▼                     ▼               │
                          │                     Postgres: ingest_db      Postgres: alerts_db    │
                          │                     (same server, separate DB and user per service) │
                          │                                                                    │
                          │  GitHub Actions self-hosted runner (pulls images from GHCR)        │
                          └────────────────────────────────────────────────────────────────────┘
```

| Component | Role |
|---|---|
| **Nginx** | The only public entry point (port 80). Routes `/api/v1/alerts*` to the alert service, every other `/api/v1/*` to ingest. `/metrics` and each service's `/health` are not exposed. |
| **Ingest service** (`services/ingest`) | Validates and stores observations, tracks sensors, manages rules (seeded from `config/rules.yaml`), runs the rule engine and publishes rule matches to Kafka. |
| **Alert service** (`services/alerts`) | Consumes rule matches, deduplicates them into alerts, runs the alert lifecycle and the auto-resolve sweeper, serves the alerts API. |
| **Kafka** | Topic `rf.rule-matches`, 2 partitions, key `rule_id|signal_key`. Decouples ingest from alert handling. |
| **PostgreSQL** | One server, two databases (`ingest_db`, `alerts_db`), one login role per service. Neither service uses the superuser. |
| **Shared code** (`models/`, `db/`, `shared/rfam_common/`) | Pydantic schemas and the message contract, table models, DB/Kafka/settings helpers. Copied into both images. |
| **Simulator** (`simulator/simulate.py`) | Standard-library traffic generator with background, persistent, burst and disappearing-emitter scenarios. |

## Data flow

1. A sensor `POST`s a batch to `/api/v1/observations`. Each item is validated. Valid items are
   stored and invalid ones are reported by index (200 / 207 / 422 / 413).
2. In a single DB transaction, ingest inserts the observations and flushes (so ids are assigned),
   then updates `sensors.last_seen` (it only moves forward).
3. The **rule engine** evaluates each in-time observation against the enabled rules. State is per
   `(rule_id, signal_key, sensor_id)` and holds only `first_ts`, `last_ts`, `confirmed`.
   - `signal_key` = frequency rounded to `SIGNAL_TOLERANCE_MHZ`.
   - Once an episode lasts `min_duration_s`, every matching observation produces a `rule_match` event.
4. Ingest publishes the events to Kafka (`acks=all`, idempotent producer), and **only then commits**
   the DB transaction. If the publish fails, it rolls back and returns 503, so the sensor can retry
   the batch without creating duplicates.
5. The alert service consumes each event:
   - It opens a new alert, or updates the active one for `(rule_id, signal_key)`.
   - It stores the observation as evidence in `alert_observations`.
   - It commits to the DB and **then** commits the Kafka offset.
6. A background sweeper resolves alerts once `now ≥ last_seen + resolve_after_s` (wall clock).

### Message contract (ingest → Kafka → alerts)

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

The message carries everything the alert service needs (rule name, severity, `resolve_after_s`,
observation), so the alert service never reads `ingest_db`.

## Data model

**ingest_db** (owned by `ingest`)
- `observations`: id, sensor_id, timestamp (event time), received_at, frequency_mhz,
  bandwidth_khz, power_dbm, lat, lon.
  - Indexes: (sensor_id, timestamp), (timestamp), (frequency_mhz).
- `sensors`: sensor_id PK, last_seen, lat, lon.
- `rules`: rule_id PK, name, type, band_min/max_mhz, threshold_dbm, min_duration_s, max_gap_s,
  resolve_after_s, severity, enabled.

**alerts_db** (owned by `alerts`)
- `alerts`: alert_id, rule_id, rule_name, signal_key, state, severity, frequency_mhz, sensor_ids,
  first_seen, last_seen, occurrences, peak_power_dbm, resolve_after_s, acknowledged_at,
  resolved_at, resolved_by.
  - **Partial unique index** on (rule_id, signal_key) where state is OPEN/ACKNOWLEDGED, so the
    database itself guarantees one active alert per signal.
- `alert_observations`: the contributing observations, copied from the message.
  - Unique (alert_id, observation_id), so a redelivered message is a no-op.

## Alert lifecycle

```
           match                ack                 sweeper / manual resolve
  (none) ───────► OPEN ───────────────► ACKNOWLEDGED ─────────────────────────► RESOLVED
                    └────────────── sweeper / manual resolve ─────────────────────┘
```
- ACK does not stop updates.
- ACK on ACKNOWLEDGED is a no-op.
- ACK or resolve on RESOLVED returns 409.
- A match that arrives after an active alert's expiry resolves the old alert and opens a new one.
- A late match for a resolved alert is attached as evidence and never reopens the alert.

## Deployment topology

- **VM:** Ubuntu 24.04 defined in code with `Vagrantfile` + `infra/provision.sh`.
  - The provision script installs Docker, ufw (22/80), bounded Docker and journald logs, a systemd
    unit `rfam.service` that brings the stack up on boot, and a GitHub Actions self-hosted runner.
- **Images:** one per service, multi-stage `python:3.12-slim`, non-root, `HEALTHCHECK` on `/health`.
- **compose** (`deploy/compose.yaml`): postgres (volume + init script), kafka (KRaft, volume),
  kafka-init (creates the topic), ingest, alerts, nginx.
  - Only Nginx publishes a port. Postgres and Kafka bind to 127.0.0.1 for local debugging only.
  - Containers start in dependency order using health checks.
- **CI/CD** (`.github/workflows/pipeline.yml`):
  - **Every PR and push:** ruff and pytest.
  - **Push to main:** build both images tagged with the commit SHA, push them to GHCR, then deploy
    on the VM's runner (pull, `compose up`) and smoke-test through Nginx.
  - **Rollback:** `workflow_dispatch` with an earlier SHA.
