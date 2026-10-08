# RF Alert Manager

Receives RF observations from sensors, detects persistent high-power signals with configurable
rules, and manages the resulting alerts (open → acknowledge → auto/manual resolve).

Two FastAPI services (**ingest** and **alerts**) communicate over **Kafka**. Each has its own
**PostgreSQL** database. **Nginx** is the single entry point.

- Design: [ARCHITECTURE.md](ARCHITECTURE.md)
- Choices and trade-offs: [DECISIONS.md](DECISIONS.md)
- How AI was used: [AI_USAGE.md](AI_USAGE.md)

## Prerequisites

- Docker with the compose plugin (Docker Desktop on Windows/macOS)
- Python 3.12 (only for the simulator and the tests)

## Run everything with Docker Compose

```bash
# 1. Configuration: copy the example, then change the passwords
cp deploy/.env.example deploy/.env

# 2. Build both images and start the stack (Postgres, Kafka, topic init, ingest, alerts, Nginx)
docker compose -f deploy/compose.yaml up -d --build

# 3. Wait until every service is "healthy" (Kafka takes ~20-30 s on first start)
docker compose -f deploy/compose.yaml ps
```

The API is served on **http://localhost** (port 80).
- If port 80 is taken, set `HTTP_PORT=8080` in `deploy/.env` and run step 2 again.
- Only Nginx is published. Postgres (`127.0.0.1:5432`) and Kafka (`127.0.0.1:9094`) are reachable
  from your machine only, for debugging.

### Check that it works

```bash
curl http://localhost/health                 # Nginx
curl http://localhost/api/v1/rules           # ingest: rules seeded from services/ingest/config/rules.yaml
curl http://localhost/api/v1/sensors         # ingest
curl http://localhost/api/v1/alerts          # alert service
```

On Windows PowerShell, use `curl.exe` instead of `curl`.

### Send a test observation by hand

```bash
curl -X POST http://localhost/api/v1/observations -H "Content-Type: application/json" -d '[
  {"sensor_id": "sensor-01", "timestamp": "2026-10-08T10:00:00Z",
   "frequency_mhz": 2437.0, "bandwidth_khz": 200, "power_dbm": -40,
   "location": {"lat": 32.08, "lon": 34.78}}
]'
```

The response is **200** when every item is accepted, **207** when some are rejected (reported by
index), **422** when none are accepted, **413** when the batch is too large, and **503** when Kafka is
unavailable (nothing is stored, so it is safe to retry).

### Run the simulator (end-to-end check)

```bash
python simulator/simulate.py --url http://localhost --watch --check
```

The simulator runs 150 s (shorten it with `--duration`) and prints alert states every 10 s. Expected result:

| Scenario | Expected |
|---|---|
| Background sensors, weak or out of band | no alert |
| Persistent emitter at 2437 MHz (sensor-03 + sensor-05) | **one** alert containing both sensors |
| 3 s burst at 2462 MHz | no alert |
| Persistent emitter stops at `--emitter-off-at` (60 s) | alert **auto-resolves** ~60 s later |

`--check` exits non-zero if the outcome is not the expected one.

### Work with alerts

```bash
curl "http://localhost/api/v1/alerts?state=OPEN"
curl http://localhost/api/v1/alerts/<alert_id>             # includes contributing observations
curl -X POST http://localhost/api/v1/alerts/<alert_id>/ack
curl -X POST http://localhost/api/v1/alerts/<alert_id>/resolve
```

### Logs, stop, reset

```bash
docker compose -f deploy/compose.yaml logs -f ingest alerts   # follow service logs
docker compose -f deploy/compose.yaml down                    # stop, keep data
docker compose -f deploy/compose.yaml down -v                 # stop and delete all data (DB + Kafka)
```

The Postgres init script creates the databases and users **only on the first start of an empty
volume**. After you change DB passwords in `.env`, run `down -v` once.

## Run the tests

The tests use SQLite and an in-memory broker, so they need no Docker, Kafka or Postgres.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
ruff check .
pytest
```

## CI/CD

`.github/workflows/pipeline.yml`:
- **Every PR and push:** ruff and pytest.
- **Push to `main`:** builds both images, tags them with the commit SHA and pushes them to GHCR,
  then deploys to the VM through a self-hosted runner and smoke-tests through Nginx.
- **Rollback:** Actions → *pipeline* → *Run workflow* with an earlier commit SHA.

## VM (work in progress)

The target VM (Ubuntu 24.04) is defined as code in `Vagrantfile` and `infra/provision.sh`.
- The provision script installs Docker, a ufw firewall (22/80 only), bounded logs, a systemd unit
  that starts the stack on boot, and the GitHub Actions runner.
- **I have not verified this setup yet** because installing Vagrant failed on my machine. Until
  then, the stack runs with Docker Compose as described above, using the same compose file the VM
  would use.
