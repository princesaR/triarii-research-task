# AI usage

**Tool:** Claude Code (CLI), used as a pair programmer.

**How I worked with it:**
- I wrote the plan and the design decisions first (`PLAN.md`), then had the AI implement one part
  at a time.
- I reviewed each part before committing, in small commits.
- I made the architecture decisions myself: two services, Kafka, Postgres with per-service
  databases and users, in-memory rule state, and Vagrant with a self-hosted runner. The AI
  implemented them and proposed details.

## Part 1: RF Alert Manager service, simulator and tests
- AI implemented the services from my plan: the rule engine, the alert lifecycle, the Kafka
  producer and consumer, the endpoints, the simulator and the tests.
- An earlier AI prototype used Redis Streams, one shared image and one shared DB. I rejected that
  structure and kept only its rule engine, lifecycle, simulator and tests. The broker, packaging
  and DB setup were redone for Kafka, two images and two databases.
- I slimmed the Kafka setup: a TCP healthcheck, small internal topics, and no dead-letter topic.

## Part 2: containers, compose, VM
- AI (Claude Code) wrote:
  - the two multi-stage Dockerfiles (non-root, HEALTHCHECK via Python urllib since slim has no curl)
  - the Nginx routing config
  - the app services in compose
  - the systemd unit in the provision script
- I had already written the Vagrantfile, the provision script, the Postgres/Kafka compose setup and
  the per-service DB users. The AI built on them without changing them.

## Part 3: CI/CD
- The AI wrote `.github/workflows/pipeline.yml`:
  - lint + test on every PR and push
  - build and push to GHCR with SHA tags
  - deploy on the self-hosted runner, with a smoke test through Nginx
  - rollback via `workflow_dispatch`

## What I verified myself
- I read every file before committing, and I can explain each design choice.
- Tests run in CI (ruff + pytest). The end-to-end behavior was checked with the simulator
