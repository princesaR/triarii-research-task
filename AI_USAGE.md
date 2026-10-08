# AI usage

## Part 2: containers, compose, VM
- AI (Claude Code) wrote the two multi-stage Dockerfiles (non-root, HEALTHCHECK via Python urllib since slim has no curl), the Nginx routing config, the app services in compose and the systemd unit in the provision script.
- I had already written the Vagrantfile, provision script, Postgres/Kafka compose setup and per-service DB users; the AI built on them without changing them.
