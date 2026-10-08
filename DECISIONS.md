# Design decisions

The time limit was 4 hours, and the spec sets the priority: *a simple service deployed through a working pipeline
beats a feature-rich service that only runs on a laptop.* Most choices below trade scale for
simplicity and explain what would change at scale.

## 1. Two services, medium separation

`services/ingest` and `services/alerts` live in one repo, with **a separate Dockerfile and image each**.
A small shared package (schemas, message contract, table models) is copied into both images.

**Why:** each service can run, be tested and be deployed on its own, and ingest load does not
  slow alert handling.

**In production** I would split them into two fully independent microservices:
- **Separate repositories** with their own CI/CD, versioning and release cycle, so one service can be deployed or rolled back without touching the other.
- **No shared code package.** `rfam_common` would become either a versioned library published to a package registry, or, better, a message contract enforced by a schema registry (e.g. JSON Schema or Avro), so producer and consumer can evolve independently.
- **Separate databases**, each service owning its own data, communicating only through Kafka.
- **Independent scaling**: ingest scales with sensor traffic, alerts with the number of partitions and consumers.



## 2. Kafka as the broker

A single broker in **KRaft mode** (no Zookeeper) with a limited JVM heap.

- **Topic:** `rf.rule-matches`, 2 partitions.
- **Message key:** `rule_id|signal_key`.

**Why Kafka:**
- A durable log with replay, so the alert service can be down and catch up.
- Partitions scale consumers.
- It is the standard for sensor and event streams.

**Why that key:** every message about one signal lands in one partition, so per-signal ordering
is preserved, which the dedup logic depends on.

**Delivery guarantees:**
- **Producer:** `acks=all` and idempotence. Ingest commits its DB transaction only after Kafka
  confirms the publish. If the publish fails, it rolls back and returns 503, and the sensor retries
  the batch with no duplicates.
- **Consumer:** auto-commit is off, and the offset is committed **after** the DB commit. That gives
  at-least-once delivery, made idempotent by a unique constraint (see 3).
- **Malformed messages** are logged with topic, partition and offset and then skipped, so one bad
  message cannot block a partition. A dead-letter topic was left out to keep the setup small.
- **Topics** are created explicitly by a `kafka-init` job, and auto-create is off, so a typo fails
  instead of silently creating a new topic.

**Alternative: Redis Streams.** Lighter and enough for this load. The prototype used it, but Kafka
gives retention, replay and a clear scaling path.

## 3. PostgreSQL: one server, two databases, one user per service

`ingest_db` and `alerts_db` are each owned by their own login role, created by
`deploy/postgres/init.sh`.

- `CONNECT` is revoked from `PUBLIC`, and no service uses the superuser.
- Passwords come from `.env` (locally) or GitHub secrets (on deploy).

**Why Postgres:**
- Range queries on time and frequency.
- Transactions, needed for "insert, publish, commit".
- **Partial unique index** on `(rule_id, signal_key) WHERE state IN ('OPEN','ACKNOWLEDGED')`, so the
  database, not application code, enforces "one active alert per signal".
- Unique `(alert_id, observation_id)` makes Kafka redelivery idempotent.

**Why one server:** everything runs on one VM anyway. Each service only knows its own
`DATABASE_URL`, so moving a database to its own server later is a configuration change.

**Why separate databases:** each service owns its data. The alert service gets everything it needs
from the Kafka message and never reads `ingest_db`.

**Rejected: Elasticsearch.**
- No transactions or unique constraints.
- Near-real-time refresh breaks the "is there already an open alert?" check.
- A heavy JVM next to Kafka on a small VM.

## 4. Rule-evaluation state in memory

A plain Python `dict`, with no Flink, PyFlink or Quix.

- **Why:** the logic is small and fully unit-testable without infrastructure.
- **What is stored:** per `(rule_id, signal_key, sensor_id)`, only `first_ts`, `last_ts` and
  `confirmed`. Observations themselves are never kept in memory.
- **Bounded memory:** stale keys are pruned periodically.
- **Cost (accepted):**
  - **State resets on restart.** After a deploy, an ongoing signal opens its alert up to
    `min_duration_s` later. No data is lost, because observations are in the DB.
  - **Ingest must run as a single replica.**
- **At scale:** a Flink job with `keyBy` and keyed state, or Quix Streams.

## 5. FastAPI, Pydantic, SQLAlchemy 2, Python 3.12

- Built-in request validation and OpenAPI docs.
- One ORM for both services.
- Unit tests run on SQLite with an in-memory broker double, so CI needs no Kafka or Postgres.

## Batch policy: partial accept

Sensors send batches. Valid items are stored and invalid ones are reported by index.

| Status | Meaning |
|---|---|
| 200 | All items accepted |
| 207 | Some items rejected |
| 422 | No items accepted |
| 413 | Batch is larger than `MAX_BATCH_SIZE` |

Rejecting a whole batch because of one bad reading would throw away good data.

**Validation rules:**
- All fields are required.
- Frequency 1–6000 MHz, bandwidth > 0, power −150..+30 dBm, valid lat/lon.
- Timestamps must be ISO-8601 in **UTC only**. Other offsets are rejected so event times compare
  safely.

## Persistence rule and late or out-of-order data

- **Per-sensor episodes.** A weak, distant sensor cannot cancel a close sensor's persistence.
- **Event time only.** The watermark is the newest timestamp seen.
  - Observations older than `watermark − MAX_LATENESS_S` (default 300 s) are **stored but not
    evaluated**.
  - Within that window, out-of-order observations widen the episode with min/max.
- **Gaps and breaks:**
  - A gap larger than `max_gap_s` restarts the episode.
  - A below-threshold reading that is newer than the episode breaks it.
  - A short burst never publishes.
- **Rule changes:** updating or deleting a rule clears its state.

## Auto-resolve uses the wall clock

A vanished emitter produces no events, so event time cannot move forward to trigger a resolve.

- The sweeper therefore resolves when `now ≥ last_seen + resolve_after_s`, and records
  `resolved_at = last_seen + resolve_after_s`.
- This assumes the sensors' clocks are NTP-synced.
- A late match for an already-resolved alert is kept as evidence and never reopens it.

## Deployment

- **VM:** local Vagrant with a GitHub self-hosted runner inside the VM.
  - The deploy needs no inbound access or SSH keys, and nothing in the cloud costs money.
  - Cloud with Terraform would be the next step.
- **Images:** tagged with the commit SHA and pushed to GHCR. The VM only pulls images and never
  builds code.
- **Rollback:** redeploy an earlier SHA through `workflow_dispatch`.
- **Startup and resource limits:**
  - systemd starts compose on boot.
  - ufw allows only 22 and 80.
  - Logs are capped (json-file 10 MB × 3, journald 200 MB).

## Not production-ready / future work

- In-memory rule state limits ingest to one replica. At scale: Flink keyed state or Quix Streams.
- One Postgres server. Split it physically, observations first (noisy neighbor, different retention).
- Observations at scale: TimescaleDB, or Kafka Connect to Elasticsearch for analytics.
- No migrations tool (`create_all`). Production would use Alembic.
- Single Kafka broker with replication factor 1, and no dead-letter topic.
- Auto-resolve uses the wall clock and assumes synced sensor clocks.
- No TLS or authentication on the API. In production that means TLS at Nginx plus API keys or mTLS
  for sensors.
- Secrets come from GitHub secrets written into `.env`. Production would use a secret manager.
- Optional rule types (`multi_sensor`, `sensor_silence`), webhooks and Grafana dashboards were not built.
