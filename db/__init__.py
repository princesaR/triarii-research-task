"""SQLAlchemy table models, one sub-package per database, one file per table.

- db.ingest -> ingest_db (owned by the ingest service)
- db.alerts -> alerts_db (owned by the alert service)

Each database has its own Base, so create_all() on one never touches the other.
The services never query each other's tables; alerts get their data from Kafka.
"""
